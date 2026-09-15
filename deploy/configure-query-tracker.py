#!/usr/bin/env python3
"""Create tracker config from existing API database URL without printing credentials.

Writes config files only. Does not migrate databases, move data, or restart services.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import tomllib
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--api-config', type=Path, required=True)
p.add_argument('--tracker-config', type=Path, required=True)
a = p.parse_args()
api_text = a.api_config.read_text()
api = tomllib.loads(api_text)
client = {'endpoint': 'http://127.0.0.1:4004', 'timeout': '2s',
          'flush-interval': '5s', 'max-buffered-identities': 1000, 'max-batch-size': 100}
if 'query-tracker-client' in api and api['query-tracker-client'] != client:
    raise SystemExit('Existing tracker client differs; review it before proceeding.')
url = urlsplit(api['database']['url'])
q = [(k, v) for k, v in parse_qsl(url.query) if k not in ('options', 'application_name')]
if any(k == 'options' for k, _ in parse_qsl(url.query)):
    raise SystemExit('Existing database options require manual merge; refusing to discard them.')
q.extend([('application_name', 'cloudbreak-query-tracker'),
          ('options', '-c lock_timeout=500ms -c statement_timeout=5000 '
           '-c max_parallel_maintenance_workers=0 -c maintenance_work_mem=65536')])
url = urlunsplit(url._replace(query=urlencode(q)))
template = Path(__file__).with_name('query-tracker-shared-host.toml').read_text()
tracker = template.replace('"__DATABASE_URL__"', json.dumps(url))
# Parse locally before writing. No credential-bearing output.
tomllib.loads(tracker)
stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
for path in (a.api_config, a.tracker_config):
    if path.exists():
        backup = path.with_name(path.name + '.backup-' + stamp)
        shutil.copy2(path, backup)
        backup.chmod(0o600)
mask = os.umask(0o027)
try:
    a.tracker_config.write_text(tracker)
    a.tracker_config.chmod(0o640)
    shutil.chown(a.tracker_config, group='cloudbreak')
    if 'query-tracker-client' not in api:
        text = '\n[query-tracker-client]\n' + ''.join(
            f'{k} = {json.dumps(v)}\n' for k, v in client.items())
        a.api_config.write_text(api_text + text)
finally:
    os.umask(mask)
print('Tracker configuration prepared; credentials were not printed. Services unchanged.')
