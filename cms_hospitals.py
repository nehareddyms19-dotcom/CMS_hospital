#!/usr/bin/env python3
"""Download CMS Hospitals CSV distributions with parallelism and durable state."""
import argparse
import concurrent.futures
import csv
import hashlib
import io
import json
import logging
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from datetime import datetime, timezone

CATALOG = 'https://data.cms.gov/provider-data/api/1/metastore/schemas/dataset/items'
LOG = logging.getLogger('cms_hospitals')

def now():
    return datetime.now(timezone.utc).isoformat()

def snake_case(value):
    value = unicodedata.normalize('NFKD', value).encode('ascii', 'ignore').decode()
    value = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', value)
    value = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', value)
    value = re.sub(r"['’]", '', value)
    return re.sub(r'[^a-zA-Z0-9]+', '_', value).strip('_').lower() or 'column'

def headers_unique(headers):
    used, result = set(), []
    for original in headers:
        base = snake_case(original)
        candidate, suffix = base, 2
        while candidate in used:
            candidate = f'{base}_{suffix}'
            suffix += 1
        used.add(candidate)
        result.append(candidate)
    return result

def request(url, timeout):
    if urlparse(url).scheme not in ('https', 'http'):
        raise ValueError('Only HTTP(S) download URLs are supported')
    for attempt in range(4):
        try:
            return urlopen(Request(url, headers={'User-Agent': 'CMS-Hospitals-Pipeline/1.0'}), timeout=timeout)
        except HTTPError as exc:
            if exc.code not in (408, 429, 500, 502, 503, 504) or attempt == 3:
                raise
            exc.close()
        except (URLError, TimeoutError):
            if attempt == 3:
                raise
        time.sleep(2 ** attempt)

def tasks_from(catalog):
    seen = set()
    for dataset in catalog:
        themes = dataset.get('theme', [])
        if isinstance(themes, str):
            themes = [themes]
        if not any(str(t).strip().casefold() == 'hospitals' for t in themes):
            continue
        identifier = str(dataset['identifier'])
        for dist in dataset.get('distribution', []):
            url = dist.get('downloadURL')
            media = str(dist.get('mediaType', '')).split(';')[0].lower()
            if not url or not (media in ('text/csv', 'application/csv') or urlparse(url).path.lower().endswith('.csv')):
                continue
            key = identifier + ':' + hashlib.sha256(url.encode()).hexdigest()[:16]
            if key in seen:
                continue
            seen.add(key)
            version = json.dumps({'modified': dataset.get('modified'), 'released': dataset.get('released'), 'url': url, 'distribution_modified': dist.get('modified')}, sort_keys=True)
            yield {'key': key, 'dataset_id': identifier, 'title': dataset.get('title', ''), 'url': url, 'modified': dataset.get('modified'), 'version': version,
                   'filename': snake_case(identifier) + '_' + hashlib.sha256(url.encode()).hexdigest()[:12] + '.csv'}

def process(task, output, timeout):
    target = output / task['filename']
    fd, temp = tempfile.mkstemp(prefix='.partial_', dir=output)
    os.close(fd)
    try:
        with request(task['url'], timeout) as response:
            # CMS CSVs use UTF-8; utf-8-sig also removes an optional BOM.
            with io.TextIOWrapper(response, encoding='utf-8-sig', newline='') as source, open(temp, 'w', encoding='utf-8', newline='') as dest:
                reader, writer = csv.reader(source), csv.writer(dest)
                original = next(reader, None)
                if not original:
                    raise ValueError('CSV has no header')
                normalized = headers_unique(original)
                writer.writerow(normalized)
                count = 0
                for row in reader:
                    if len(row) != len(original):
                        raise ValueError(f'CSV row {reader.line_num} has {len(row)} fields; expected {len(original)}')
                    writer.writerow(row)
                    count += 1
        digest = hashlib.sha256()
        with open(temp, 'rb') as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b''):
                digest.update(chunk)
        os.replace(temp, target)
        return {'rows': count, 'columns': len(original), 'sha256': digest.hexdigest(), 'path': str(target.resolve()), 'header_mapping': list(zip(original, normalized))}
    finally:
        if os.path.exists(temp):
            os.unlink(temp)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path('output'))
    parser.add_argument('--state', type=Path, default=Path('state/cms.sqlite'))
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--timeout', type=int, default=90)
    parser.add_argument('--limit', type=int, help='Sample run only; omit to download all Hospitals datasets')
    parser.add_argument('--force', action='store_true', help='Download regardless of saved metadata')
    args = parser.parse_args()
    if args.workers < 1 or args.timeout < 1 or (args.limit is not None and args.limit < 1):
        parser.error('workers, timeout and limit must be positive')
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    args.output = args.output.resolve()
    args.state = args.state.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    args.state.parent.mkdir(parents=True, exist_ok=True)
    lock = args.state.with_suffix('.lock')
    try:
        lock_fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        LOG.error('Another run holds %s. If a process crashed, confirm it has stopped before removing this lock.', lock)
        return 2
    os.write(lock_fd, str(os.getpid()).encode())
    os.close(lock_fd)
    db = None
    try:
        db = sqlite3.connect(args.state)
        db.executescript('''CREATE TABLE IF NOT EXISTS files (resource_key TEXT PRIMARY KEY, version TEXT NOT NULL, path TEXT NOT NULL, downloaded_at TEXT NOT NULL, details TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS runs (id INTEGER PRIMARY KEY, started_at TEXT, finished_at TEXT, status TEXT, summary TEXT);
        CREATE TABLE IF NOT EXISTS run_items (run_id INTEGER, resource_key TEXT, status TEXT, details TEXT);''')
        run_id = db.execute('INSERT INTO runs(started_at,status) VALUES (?,?)', (now(), 'running')).lastrowid
        db.commit()
        summary = {'run_id': run_id, 'started_at': now(), 'downloaded': 0, 'skipped': 0, 'failed': 0, 'sample_run': args.limit is not None, 'items': []}
        try:
            with request(CATALOG, args.timeout) as response:
                catalog = json.load(response)
            if not isinstance(catalog, list):
                raise ValueError('Expected CMS catalog to be a JSON array')
            tasks = list(tasks_from(catalog))
            if not tasks:
                raise ValueError('No Hospitals CSV distributions found; check the catalog schema')
            summary['discovered_csv_distributions'] = len(tasks)
            if args.limit:
                tasks = tasks[:args.limit]
            pending = []
            for task in tasks:
                previous = db.execute('SELECT version,path FROM files WHERE resource_key=?', (task['key'],)).fetchone()
                # Without a modified timestamp, re-download conservatively.
                if not args.force and task['modified'] and previous and previous[0] == task['version'] and Path(previous[1]) == args.output / task['filename'] and Path(previous[1]).is_file():
                    summary['skipped'] += 1
                    item = {'dataset_id': task['dataset_id'], 'status': 'skipped', 'title': task['title']}
                    summary['items'].append(item)
                    db.execute('INSERT INTO run_items VALUES (?,?,?,?)', (run_id, task['key'], 'skipped', json.dumps(item)))
                else:
                    pending.append(task)
            db.commit()
            with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
                futures = {pool.submit(process, task, args.output, args.timeout): task for task in pending}
                for future in concurrent.futures.as_completed(futures):
                    task = futures[future]
                    item = {'dataset_id': task['dataset_id'], 'title': task['title'], 'url': task['url']}
                    try:
                        details = future.result()
                        item.update(details, status='downloaded')
                        db.execute('INSERT OR REPLACE INTO files VALUES (?,?,?,?,?)', (task['key'], task['version'], details['path'], now(), json.dumps(details)))
                        summary['downloaded'] += 1
                        LOG.info('Downloaded %s: %s rows', task['dataset_id'], details['rows'])
                    except Exception as exc:
                        item.update(status='failed', error=str(exc))
                        summary['failed'] += 1
                        LOG.error('Failed %s: %s', task['dataset_id'], exc)
                    summary['items'].append(item)
                    db.execute('INSERT INTO run_items VALUES (?,?,?,?)', (run_id, task['key'], item['status'], json.dumps(item)))
                    db.commit()
            status = 'partial_failure' if summary['failed'] else 'success'
        except Exception as exc:
            summary['error'] = str(exc)
            status = 'failed'
            LOG.exception('Run failed')
        summary.update(status=status, finished_at=now())
        db.execute('UPDATE runs SET finished_at=?,status=?,summary=? WHERE id=?', (summary['finished_at'], status, json.dumps(summary), run_id))
        db.commit()
        report = args.output / f'run_{run_id}.json'
        report.write_text(json.dumps(summary, indent=2), encoding='utf-8')
        LOG.info('Run %s: %s downloaded, %s skipped, %s failed. Report: %s', run_id, summary['downloaded'], summary['skipped'], summary['failed'], report)
        return 0 if status == 'success' else 1
    finally:
        if db:
            db.close()
        lock.unlink(missing_ok=True)

if __name__ == '__main__':
    sys.exit(main())
