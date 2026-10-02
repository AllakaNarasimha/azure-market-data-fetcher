import json
import os
import fnmatch
from pathlib import Path

# Load local.settings.json values into env
cfg = json.loads(Path('local.settings.json').read_text())
for k, v in cfg.get('Values', {}).items():
    os.environ.setdefault(k, str(v))

# Ensure repository root is on sys.path so local modules can be imported when
# running this script from the scripts/ directory.
import sys
repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from storage.market_data_storage_client import MarketDataStorageClient
from utils.blob_utils import BlobUtils

c = MarketDataStorageClient()
container = BlobUtils.market_data_cache_blob()


def get_matches(prefix):
    folders = c.list_folders_recursively(container, prefix)
    matches = []
    for folder in folders:
        try:
            files = c.list_files_in_subfolder(container, folder)
            for f in files:
                if fnmatch.fnmatch(Path(f).name, '*.parquet'):
                    matches.append(f)
        except Exception as e:
            print('warning listing', folder, e)
    return matches


all_matches = get_matches('candles/')
axis_prefix = 'candles/symbol=NSE%3AAXISBANK-EQ/'
axis_matches = [m for m in all_matches if m.startswith(axis_prefix)]

Path('candles_parquet_list.txt').write_text('\n'.join(all_matches))
Path('axisbank_parquet_list.txt').write_text('\n'.join(axis_matches))

print(f'Wrote {len(all_matches)} to candles_parquet_list.txt')
print(f'Wrote {len(axis_matches)} to axisbank_parquet_list.txt')
