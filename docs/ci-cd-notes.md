# CI/CD Deployment Notes (Azure Functions, Python)

Lessons learned debugging the `marketdata-func-0916-narasimha` Function App deploy
pipeline (`Pipeline-Templates` repo: `master-deploy.yml` + `build/python.yml`).

## How this app is deployed

- `.azure-pipelines.yml` calls the shared `master-deploy.yml@templates` template with
  `techStack: python`.
- The `Deploy` stage uses `AzureFunctionApp@1` with a pre-built zip artifact. This sets
  `WEBSITE_RUN_FROM_PACKAGE` to an **external blob SAS URL**. This mode mounts the zip
  read-only and **skips Oryx build entirely** — `SCM_DO_BUILD_DURING_DEPLOYMENT=true`
  has no effect here. Any Python dependencies must already be correctly vendored
  inside the zip before it's uploaded.

## Bug #1: dependencies installed into a venv, never on the runtime's sys.path

`build/python.yml` originally installed dependencies into a virtual environment
(`antenv`). `ArchiveFiles@2` zipped the whole working directory (including `antenv`),
but the Azure Functions Python worker only scans
`/home/site/wwwroot/.python_packages/lib/site-packages` on `sys.path` — never a venv
folder. Result: `ModuleNotFoundError: No module named 'pytz'` and `0 functions loaded`
in production, even though `pytz` was listed correctly in `requirements.txt`.

**Fix:** install straight into `.python_packages/lib/site-packages`:

```yaml
- script: |
    python -m pip install --upgrade pip
    pip install -r requirements.txt --target="$(System.DefaultWorkingDirectory)/.python_packages/lib/site-packages"
  displayName: 'Install Dependencies'
```

## Bug #2: build agent glibc newer than the Function runtime container

After fixing bug #1, deploys failed with:

```
ImportError: /lib/x86_64-linux-gnu/libc.so.6: version `GLIBC_2.33' not found
(required by .../cryptography/hazmat/bindings/_rust.abi3.so)
```

`pip install` on the `ubuntu-latest` build agent resolved a `cryptography` wheel built
for a newer manylinux tag than the Function App's `Python|3.11` Linux runtime
container supports. Building on a different OS than the one that runs the code is
exactly the class of bug Oryx remote build exists to avoid.

**Fix:** pin pip's wheel resolution to the runtime's platform tag:

```yaml
- script: |
    python -m pip install --upgrade pip
    pip install -r requirements.txt \
      --target="$(System.DefaultWorkingDirectory)/.python_packages/lib/site-packages" \
      --platform manylinux2014_x86_64 \
      --implementation cp \
      --python-version 3.11 \
      --only-binary=:all: \
      --upgrade
  displayName: 'Install Dependencies'
```

## Standard playbook for this class of issue (build vs. runtime environment mismatch)

1. **Prefer Oryx remote build** over vendoring `.python_packages` yourself — deploy via
   a real zip-push with `SCM_DO_BUILD_DURING_DEPLOYMENT=true` (not an external-URL
   run-from-package), so the build happens inside a container matching the runtime.
2. **If vendoring is required**, always pin `--platform` / `--python-version` /
   `--implementation` / `--only-binary=:all:` to match the Function App's
   `LinuxFxVersion` — never let pip resolve wheels based on the build agent's own OS.
3. **Pin exact dependency versions** in `requirements.txt` (not just names) for
   reproducible builds — this avoids "a transitive dependency silently upgraded to a
   version requiring a newer glibc" breaking a previously-working pipeline.
4. **Add a CI smoke test** that imports the app inside the real runtime image before
   deploying:
   ```yaml
   - script: |
       docker run --rm \
         -v "$(System.DefaultWorkingDirectory)":/home/site/wwwroot \
         -w /home/site/wwwroot \
         mcr.microsoft.com/azure-functions/python:4-python3.11 \
         python -c "import function_app; print('OK: function_app imports cleanly')"
     displayName: 'Smoke Test: import function_app in Functions runtime image'
   ```
5. **Add a post-deploy health check** that fails the pipeline (not just logs a
   warning) if the deployed app indexes 0 functions:
   ```yaml
   - task: AzureCLI@2
     displayName: 'Post-Deploy Health Check: verify functions indexed'
     inputs:
       azureSubscription: '${{ parameters.azureServiceConnection }}'
       scriptType: bash
       scriptLocation: inlineScript
       inlineScript: |
         set -e
         sleep 30
         FUNC_COUNT=$(az functionapp function list \
           --name '${{ parameters.appName }}' \
           --resource-group '${{ parameters.resourceGroupName }}' \
           --query "length(@)" -o tsv)
         if [ "$FUNC_COUNT" -eq "0" ]; then
           echo "##vso[task.logissue type=error]Deployment succeeded but 0 functions were indexed."
           exit 1
         fi
   ```

## Diagnosing "0 functions loaded" in production

- Get the App Insights resource ID from `functionapp_get` → tags →
  `hidden-link: /app-insights-resource-id`.
- Query it with `monitor_resource_log_query` (table `traces`, union with `exceptions`)
  — not `monitor_workspace_log_query` (that needs a Log Analytics workspace, not a
  classic App Insights resource).
- `severityLevel` in the `traces` table is a **string**. Use `toint(severityLevel) >= N`
  in KQL — a raw numeric comparison fails with `SEM0064` ("Cannot compare values of
  types int and string").

## Azure CLI command reference (used this session)

Fixed values for this app — reuse these in every command below:

```powershell
$app = "marketdata-func-0916-narasimha"
$rg  = "rg-marketdata-dev-eastus"
$sub = "0f5c3e28-585e-405a-92b0-b2894fd77076"
$kv  = "kv-marketdata-dev-eus"
```

### Fetch the latest logs (the one you asked about)

Live-tail logs directly from the Function App (equivalent to watching the log
stream in the portal) — simplest option, no query language needed:

```powershell
az functionapp log tail --name $app --resource-group $rg
```

For structured/historical queries (filter by keyword, time range, severity),
query Application Insights directly with KQL via the `az monitor app-insights`
extension (installs on first use):

```powershell
# Find the App Insights resource name/app-id once:
az monitor app-insights component show --resource-group $rg --query "[].{name:name, appId:appId}" -o table

# Then query it (replace <appId> with the value above):
az monitor app-insights query `
  --app <appId> `
  --analytics-query "traces | where timestamp > ago(1h) | order by timestamp desc | project timestamp, message, severityLevel" `
  -o table
```

Useful query variants (swap into `--analytics-query`):
- Errors only: `traces | where toint(severityLevel) >= 3 | order by timestamp desc`
- Exceptions table: `exceptions | order by timestamp desc | project timestamp, outerMessage`
- Keyword search: `traces | where message has 'TOTP' | order by timestamp desc`

### Full command list from this session

| Command | What it's for | How to run | Inputs to set |
|---|---|---|---|
| `az functionapp show` | Get app status, state, last-modified time | `az functionapp show --name $app --resource-group $rg --query "{lastModified:lastModifiedTimeUtc, state:state}" -o table` | `--name`, `--resource-group` |
| `az functionapp config show` | Check the configured runtime stack (e.g. `Python\|3.11`) | `az functionapp config show --name $app --resource-group $rg --query "{linuxFxVersion:linuxFxVersion}" -o table` | `--name`, `--resource-group` |
| `az functionapp config appsettings list` | List/filter app settings (env vars) | `az functionapp config appsettings list --name $app --resource-group $rg --query "[?contains(name,'TEST_MODE')]" -o table` | `--name`, `--resource-group`, optional `--query` JMESPath filter |
| `az functionapp config appsettings set` | Add/update app settings | `az functionapp config appsettings set --name $app --resource-group $rg --settings "TEST_MODE=false"` | `--name`, `--resource-group`, `--settings "KEY=VALUE"` (space-separated for multiple; use a JSON file with `--settings "@file.json"` when values contain `@`/`()` that PowerShell mis-parses) |
| `az functionapp identity show` | Get the Function App's managed identity principal ID | `az functionapp identity show --name $app --resource-group $rg -o table` | `--name`, `--resource-group` |
| `az keyvault show` | Check if a Key Vault uses RBAC or access policies | `az keyvault show --name $kv --query "{rbac:properties.enableRbacAuthorization}" -o json` | `--name` (vault name) |
| `az role assignment list` | Check existing RBAC role assignments on a resource | `az role assignment list --assignee <principalId> --scope <resourceId> -o table` | `--assignee` (principal ID), `--scope` (full resource ID) |
| `az role assignment create` | Grant an RBAC role (e.g. Key Vault access) | `az role assignment create --assignee <principalId> --role "Key Vault Secrets User" --scope <resourceId>` | `--assignee`, `--role` (exact role name), `--scope` |
| `az functionapp restart` | Restart the app to pick up new app settings/deploys | `az functionapp restart --name $app --resource-group $rg` | `--name`, `--resource-group` |
| `az functionapp log tail` | Live-stream logs | `az functionapp log tail --name $app --resource-group $rg` | `--name`, `--resource-group` |
| `az monitor app-insights query` | Run a KQL query against App Insights (history, filters) | See "Fetch the latest logs" above | `--app` (App Insights app ID, not the Function App name), `--analytics-query` (KQL) |

**Gotchas hit this session, worth remembering:**
- PowerShell mangles `@Microsoft.KeyVault(SecretUri=...)` values passed inline to
  `--settings` (parentheses get parsed as PowerShell syntax). Write them to a JSON
  file (`{"KEY": "value"}`) and pass `--settings "@path\to\file.json"` instead.
- `az role assignment create` is idempotent-ish but will error if the exact same
  assignment already exists — check with `az role assignment list` first.
- Key Vault references (`@Microsoft.KeyVault(...)`) only resolve if the Function
  App's managed identity has a role like **Key Vault Secrets User** on that vault
  (for RBAC-mode vaults) — check `az keyvault show` for `enableRbacAuthorization`
  before assuming access-policy-based grants apply.

# 1. Grant the Function App's managed identity permission to read secrets
az role assignment create `
  --assignee 95d4197e-0357-42aa-a343-8ea15d0562b0 `
  --role "Key Vault Secrets User" `
  --scope /subscriptions/0f5c3e28-585e-405a-92b0-b2894fd77076/resourceGroups/rg-marketdata-dev-eastus/providers/Microsoft.KeyVault/vaults/kv-marketdata-dev-eus

# 2. Fix the app settings to use real Key Vault references
az functionapp config appsettings set --name marketdata-func-0916-narasimha --resource-group rg-marketdata-dev-eastus --settings `
  "FYERS_TOTP=@Microsoft.KeyVault(SecretUri=https://kv-marketdata-dev-eus.vault.azure.net/secrets/fyers-totp/3aa7fc52721e40cfad69c47055339e28)" `
  "FYERS_PIN=@Microsoft.KeyVault(SecretUri=https://kv-marketdata-dev-eus.vault.azure.net/secrets/fyers-pin/675c29810abd438690ff009b4ccf301a)" `
  "FYERS_APP=@Microsoft.KeyVault(SecretUri=https://kv-marketdata-dev-eus.vault.azure.net/secrets/fyers-app/77a3b3345f20469eacfe8e7bb96902e5)" `
  "DHAN_PIN=@Microsoft.KeyVault(SecretUri=https://kv-marketdata-dev-eus.vault.azure.net/secrets/dhan-pin/f3d3e133e19046d884a1f28d0be5c897)" `
  "DHAN_TOTP=@Microsoft.KeyVault(SecretUri=https://kv-marketdata-dev-eus.vault.azure.net/secrets/dhan-totp/038d2e40589540d29f84f4d59b7cc1bc)"

  # Restart the server
  az functionapp restart --name marketdata-func-0916-narasimha --resource-group rg-marketdata-dev-eastus

  # update the test mode false
  az functionapp config appsettings set --name marketdata-func-0916-narasimha --resource-group rg-marketdata-dev-eastus --settings "TEST_MODE=false" -o table