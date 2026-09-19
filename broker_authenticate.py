from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlparse

import pyotp
import requests
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from dhanhq import DhanContext, DhanLogin, dhanhq
from fyers_apiv3 import fyersModel

from blob_utils import BlobUtils, is_running_locally

logger = logging.getLogger(__name__)


class ConfigStore:
    LOCAL_SETTINGS_PATH = Path(__file__).parent / "local.settings.json"

    def __init__(self):
        self.is_local = is_running_locally()
        self.key_vault_url = os.getenv("KEY_VAULT_URL")
        # When running locally, ensure values from local.settings.json are
        # loaded into the process environment so standalone scripts (or
        # modules run directly) can read config via os.getenv.
        if self.is_local and self.LOCAL_SETTINGS_PATH.exists():
            try:
                data = json.loads(self.LOCAL_SETTINGS_PATH.read_text())
                for k, v in data.get("Values", {}).items():
                    if k not in os.environ:
                        os.environ[k] = v
            except Exception:
                logger.exception("Failed to load local.settings.json into environment")

    def load(self, env_var: str) -> dict:
        raw = os.getenv(env_var)
        if not raw:
            raise RuntimeError(f"{env_var} is not configured")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # Tolerate an accidentally double-escaped App Setting value (e.g.
            # '{\"key\":\"value\"}' pasted verbatim from local.settings.json)
            # by unescaping once and retrying before giving up.
            return json.loads(raw.replace('\\"', '"'))

    def update_value(self, env_var: str, key: str, value: str) -> None:
        try:
            config = self.load(env_var)
            config[key] = value
            if self.is_local:
                self._update_local_settings(env_var, config)
            else:
                self._update_key_vault_secret(env_var, config)
        except Exception:
            logger.exception("Failed to persist updated %s.%s", env_var, key)

    def _update_local_settings(self, env_var: str, config: dict) -> None:
        data = json.loads(self.LOCAL_SETTINGS_PATH.read_text())
        data.setdefault("Values", {})[env_var] = json.dumps(config)
        self.LOCAL_SETTINGS_PATH.write_text(json.dumps(data, indent=2))
        os.environ[env_var] = json.dumps(config)
        logger.info("Updated %s in local.settings.json", env_var)

    def _update_key_vault_secret(self, env_var: str, config: dict) -> None:
        if not self.key_vault_url:
            logger.warning("KEY_VAULT_URL not set; skipping Key Vault update for %s", env_var)
            return
        secret_name = env_var.lower().replace("_", "-")
        client = SecretClient(vault_url=self.key_vault_url, credential=DefaultAzureCredential())
        client.set_secret(secret_name, json.dumps(config))
        os.environ[env_var] = json.dumps(config)
        logger.info("Updated secret '%s' in Key Vault (new version)", secret_name)


class TokenCache:
    LOCAL_DIR = Path(__file__).parent / "local_data" / "broker_tokens"

    def __init__(self):
        self.is_local = is_running_locally()
        self._container_client = None
        if not self.is_local:
            self._container_client = BlobUtils.get_container_client(BlobUtils.BROKER_TOKEN_BLOB)

    def load(self, broker: str) -> Optional[dict]:
        try:
            if self.is_local:
                path = self.LOCAL_DIR / f"{broker}.json"
                return json.loads(path.read_text()) if path.exists() else None
            if not self._container_client:
                return None
            blob = self._container_client.get_blob_client(f"{broker}.json")
            return json.loads(blob.download_blob().readall())
        except Exception:
            return None

    def save(self, broker: str, token_data: dict) -> None:
        try:
            if self.is_local:
                self.LOCAL_DIR.mkdir(parents=True, exist_ok=True)
                (self.LOCAL_DIR / f"{broker}.json").write_text(json.dumps(token_data))
                return
            if not self._container_client:
                logger.warning("MARKET_STORAGE_CONNECTION not set; skipping %s token cache", broker)
                return
            blob = self._container_client.get_blob_client(f"{broker}.json")
            blob.upload_blob(json.dumps(token_data), overwrite=True)
        except Exception:
            logger.exception("Failed to cache %s token", broker)


class Totp:
    @staticmethod
    def compute(totp_secret: Optional[str]) -> Optional[str]:
        if not totp_secret:
            return None
        if len(totp_secret) == 6 and totp_secret.isdigit():
            return totp_secret
        try:
            return pyotp.TOTP(totp_secret).now()
        except Exception:
            logger.exception("Failed to compute TOTP code")
        return None

class BrokerConfig:
    ENV_VAR: str = ""
    FIELDS: dict = {}

    def __init__(self):
        data = ConfigStore().load(self.ENV_VAR)
        mapping = getattr(self, "MAPPING", {})
        for field, default in self.FIELDS.items():
            # Primary source: value inside the JSON config (data)
            value = data.get(field, default)
            # If absent/empty and a mapping exists, try the mapped env var
            if (value is None or value == "") and field in mapping:
                mapped_env = mapping[field]
                env_val = os.getenv(mapped_env)
                if env_val is not None:
                    value = env_val
            setattr(self, field, value)


class DhanConfig(BrokerConfig):
    ENV_VAR = "DHAN_CONFIG"
    FIELDS = {
        "client_id": None,
        "pin": None,
        "totp_secret": None,
        "base_url": "https://api.dhan.co/v2",
    }


class FyersConfig(BrokerConfig):
    ENV_VAR = "FYERS_CONFIG"
    FIELDS = {
        "app_id": None,
        "client_id": None,
        "pin": None,
        "totp_secret": None,
        "app_secret": None,
        "callback_url": "http://localhost:8000",
    }
    # Map logical field names to alternate environment variables or secrets.
    MAPPING = {
        "pin": "FYERS_PIN",
        "app_secret": "FYERS_APP",
        "totp_secret": "FYERS_TOTP"
    }

class DhanTOTPAuthenticator:
    def __init__(self, config: Optional[DhanConfig] = None):
        self.config = config or DhanConfig()

    def _is_valid(self, access_token: str) -> bool:
        try:
            resp = requests.get(
                f"{self.config.base_url}/profile",
                headers={"access-token": access_token},
                timeout=10,
            )
            return resp.status_code == 200
        except Exception:
            return False

    def get_access_token(self, force: bool = False) -> str:
        cache = TokenCache()
        if not force:
            cached = cache.load("dhan")
            if cached and cached.get("accessToken") and self._is_valid(cached["accessToken"]):
                return cached["accessToken"]

        if not (self.config.client_id and self.config.pin and self.config.totp_secret):
            raise RuntimeError("DHAN_CLIENT_ID, DHAN_PIN and DHAN_TOTP_SECRET must be configured")

        totp = Totp.compute(self.config.totp_secret)
        if not totp:
            raise RuntimeError("Unable to compute Dhan TOTP code; check DHAN_TOTP_SECRET")

        dhan_login = DhanLogin(self.config.client_id)
        response = dhan_login.generate_token(self.config.pin, totp)
        if not isinstance(response, dict) or not response.get("accessToken"):
            raise RuntimeError(f"Dhan token generation failed: {response}")

        logger.info("Dhan access token generated. Expiry: %s", response.get("expiryTime", "N/A"))
        ConfigStore().update_value(DhanConfig.ENV_VAR, "totp", totp)
        cache.save("dhan", {"accessToken": response["accessToken"], "expiryTime": response.get("expiryTime")})
        return response["accessToken"]

    def get_client(self):
        access_token = self.get_access_token()
        dhan_context = DhanContext(self.config.client_id, access_token)
        return dhanhq(dhan_context)


class FyersTOTPAuthenticator:
    def __init__(self, config: Optional[FyersConfig] = None):
        self.config = config or FyersConfig()

    @staticmethod
    def _b64(value: str) -> str:
        return base64.b64encode(str(value).encode("ascii")).decode("ascii")

    def _login_with_totp(self) -> str:
        cfg = self.config
        if not (cfg.client_id and cfg.pin and cfg.totp_secret):
            raise RuntimeError("FYERS_CLIENT_ID, FYERS_PIN and FYERS_TOTP_SECRET must be configured")

        session = requests.Session()
        resp = session.post(
            "https://api-t2.fyers.in/vagator/v2/send_login_otp_v2",
            json={"fy_id": self._b64(cfg.client_id), "app_id": "2"},
            timeout=10,
        )
        data = resp.json() if resp.content else {}
        request_key = data.get("request_key")
        if resp.status_code != 200 or not request_key:
            raise RuntimeError(f"Fyers send_login_otp failed: {resp.status_code} {data}")

        request_key_2 = None
        last_error = None
        for _ in range(3):
            totp_code = Totp.compute(cfg.totp_secret)
            if not totp_code:
                raise RuntimeError("Unable to compute TOTP code for Fyers login")
            resp = session.post(
                "https://api-t2.fyers.in/vagator/v2/verify_otp",
                json={"request_key": request_key, "otp": totp_code},
                timeout=10,
            )
            data = resp.json() if resp.content else {}
            if resp.status_code == 200 and data.get("request_key"):
                request_key_2 = data["request_key"]
                break
            last_error = data
            time.sleep(1)
        if not request_key_2:
            raise RuntimeError(f"Fyers verify_otp failed: {last_error}")

        resp = session.post(
            "https://api-t2.fyers.in/vagator/v2/verify_pin_v2",
            json={"request_key": request_key_2, "identity_type": "pin", "identifier": self._b64(cfg.pin)},
            timeout=10,
        )
        data = resp.json() if resp.content else {}
        session_token = (data.get("data") or {}).get("access_token")
        if resp.status_code != 200 or not session_token:
            raise RuntimeError(f"Fyers verify_pin failed: {resp.status_code} {data}")
        session.headers.update({"authorization": f"Bearer {session_token}"})

        base_app_id, _, app_type = cfg.app_id.partition("-")
        payload = {
            "fyers_id": cfg.client_id,
            "app_id": base_app_id,
            "redirect_uri": cfg.callback_url,
            "appType": app_type or "100",
            "code_challenge": "",
            "state": "state",
            "scope": "",
            "nonce": "",
            "response_type": "code",
            "create_cookie": True,
        }
        resp = session.post("https://api-t1.fyers.in/api/v3/token", json=payload, timeout=10)
        data = resp.json() if resp.content else {}
        url = data.get("Url")
        if not url:
            raise RuntimeError(f"Fyers token exchange failed: {resp.status_code} {data}")
        auth_code = parse_qs(urlparse(url).query).get("auth_code", [None])[0]
        if not auth_code:
            raise RuntimeError(f"auth_code missing from redirect URL: {url}")

        logger.info("Fyers headless TOTP login succeeded")
        return auth_code

    def _exchange_code(self, auth_code: str) -> str:
        cfg = self.config
        try:
            session = fyersModel.SessionModel(
                client_id=cfg.app_id,
                redirect_uri=cfg.callback_url,
                response_type="code",
                state="state",
                secret_key=cfg.app_secret,
                grant_type="authorization_code",
            )
            session.set_token(auth_code)
            result = session.generate_token()
            access_token = result.get("access_token")
            if access_token:
                return access_token
            logger.warning("Fyers SDK token exchange response: %s", result.get("message", result))
        except Exception:
            logger.exception("Fyers SDK token exchange failed")

        app_id_hash = hashlib.sha256(f"{cfg.app_id}:{cfg.app_secret}".encode()).hexdigest()
        payload = {"grant_type": "authorization_code", "appIdHash": app_id_hash, "code": auth_code}
        resp = requests.post("https://api-t1.fyers.in/api/v3/validate-authcode", json=payload, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            access_token = data.get("access_token")
            if access_token:
                return access_token
            raise RuntimeError(f"Fyers token exchange response: {data.get('message', 'no message')}")
        raise RuntimeError(f"Fyers token exchange failed: {resp.status_code} {resp.text}")

    def _is_valid(self, raw_token: str) -> bool:
        try:
            resp = requests.get(
                "https://api-t1.fyers.in/api/v3/profile",
                headers={"Authorization": f"{self.config.app_id}:{raw_token}"},
                timeout=10,
            )
            return resp.status_code == 200 and (resp.json() or {}).get("s") == "ok"
        except Exception:
            return False

    def get_access_token(self, force: bool = False) -> str:
        cache = TokenCache()
        if not force:
            cached = cache.load("fyers")
            raw_token = cached.get("accessToken") if cached else None
            if raw_token and self._is_valid(raw_token):
                return f"{self.config.app_id}:{raw_token}"

        auth_code = self._login_with_totp()
        raw_token = self._exchange_code(auth_code)
        cache.save("fyers", {"accessToken": raw_token})
        return f"{self.config.app_id}:{raw_token}"


class BrokerAuth:
    def __init__(self):
        self._dhan_auth = None
        self._fyers_auth = None

    def get_dhan_client(self, force: bool = False):
        if self._dhan_auth is None:
            self._dhan_auth = DhanTOTPAuthenticator()
        if force:
            self._dhan_auth.get_access_token(force=True)
        return self._dhan_auth.get_client()

    def get_fyers_client(self, force: bool = False):
        cfg = FyersConfig()
        full = self.get_fyers_access_token(force=force)
        parts = full.split(':', 1)
        token = parts[1] if len(parts) > 1 else parts[0]
        # fyers_apiv3 writes fyersApi.log/fyersRequests.log relative to CWD when
        # log_path is falsy; /home/site/wwwroot is read-only in Azure, so point
        # it at a writable temp directory instead.
        client = fyersModel.FyersModel(
            token=token, is_async=False, client_id=cfg.app_id, log_path=tempfile.gettempdir()
        )
        return client

    def get_fyers_session(self, force: bool = False) -> requests.Session:
        if self._fyers_auth is None:
            self._fyers_auth = FyersTOTPAuthenticator()
        raw = self._fyers_auth.get_access_token(force=force)
        session = requests.Session()
        session.headers.update({"Authorization": raw, "Content-Type": "application/json"})
        return session

    def get_dhan_access_token(self, force: bool = False) -> str:
        if self._dhan_auth is None:
            self._dhan_auth = DhanTOTPAuthenticator()
        return self._dhan_auth.get_access_token(force=force)

    def get_fyers_access_token(self, force: bool = False) -> str:
        if self._fyers_auth is None:
            self._fyers_auth = FyersTOTPAuthenticator()
        return self._fyers_auth.get_access_token(force=force)


def main() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if is_running_locally():
        handlers.append(logging.FileHandler(Path(__file__).parent / "authenticate.log", encoding="utf-8"))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s", handlers=handlers)

    try:
        dhan_token = DhanTOTPAuthenticator().get_access_token()
        logger.info("Dhan access token: %s...", dhan_token[:20])
    except Exception as e:
        logger.error("Dhan authentication failed: %s", e)

    try:
        fyers_token = FyersTOTPAuthenticator().get_access_token()
        logger.info("Fyers access token: %s...", fyers_token[:20])
    except Exception as e:
        logger.error("Fyers authentication failed: %s", e)


if __name__ == "__main__":
    main()




    
