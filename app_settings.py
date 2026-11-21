import logging
import pytz
from datetime import datetime

from utils.blob_utils import BlobUtils
from utils.market_times import MarketTimes
from utils.env_config import EnvConfig
from schedulers.scheduler_utils import calculate_dynamic_option_chain_cron

# Load local settings early so overrides are available everywhere.
EnvConfig.load_local_settings()

# Timezone and runtime mode
IST_TZ = pytz.timezone(MarketTimes.timezone())
TEST_MODE = EnvConfig.test_mode()
# Default true: a TEST_MODE left on by mistake still runs the normal schedule on trading days.
RUN_ON_MARKET_HOURS = EnvConfig.run_on_market_hours()
USE_PROD_SCHEDULE = (not TEST_MODE) or RUN_ON_MARKET_HOURS
IS_RUNNING_LOCALLY = BlobUtils.is_running_locally()
MARKET_OPEN_TIME = MarketTimes.open()
MARKET_CLOSE_TIME = MarketTimes.close()

# Initialize TEST_MODE window if enabled
if TEST_MODE:
    EnvConfig.init_test_mode_window(datetime.now(IST_TZ))

# Symbols and history defaults
OPTION_CHAIN_SYMBOLS = EnvConfig.option_chain_symbols()
WATCHLIST_SYMBOLS = EnvConfig.watchlist_symbols()
HISTORY_DAYS = EnvConfig.history_days()
HISTORY_INTERVAL = EnvConfig.history_interval()

# Parallelism / cron calculation
PARALLEL_BATCH_SIZE = int(EnvConfig.env("PARALLEL_BATCH_SIZE", "5") or "5")

_option_chain_weekday_field = "1-5" if USE_PROD_SCHEDULE else "*"
# Azure timer hours default to UTC, so keep this window aligned with IST market hours.
_option_chain_hour_field = "3-10" if USE_PROD_SCHEDULE else "*"

DYNAMIC_OPTION_CHAIN_CRON = calculate_dynamic_option_chain_cron(
    len(OPTION_CHAIN_SYMBOLS), PARALLEL_BATCH_SIZE, weekday_field=_option_chain_weekday_field, hour_field=_option_chain_hour_field
)

# Log startup environment for visibility
logging.info(
    "Startup env: TEST_MODE=%r, RUN_ON_MARKET_HOURS=%r, WEBSITE_SITE_NAME=%r, WEBSITE_SLOT_NAME=%r, ENVIRONMENT=%r",
    EnvConfig.env("TEST_MODE"),
    RUN_ON_MARKET_HOURS,
    BlobUtils.website_site_name(),
    BlobUtils.website_slot_name(),
    BlobUtils.environment(),
)
