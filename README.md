# CMS Hospitals CSV pipeline

A portable Python 3.9+ command line job that discovers every CSV distribution with the exact CMS theme `Hospitals`, downloads and transforms files concurrently, and retains successful resource versions and run history in SQLite.

## Run

No third-party packages are required. Python standard library only.

```bash
python cms_hospitals.py --output output --state state/cms.sqlite --workers 4
```

Omit `--limit` for the complete job. For a quick sample use `--limit 1`. Run the same command again to check incremental behavior. `--force` refreshes every selected file. A failed run returns a nonzero exit code. Successful resources are committed individually; failed resources are eligible for retry on the next run.

## How it works

1. Fetch the CMS provider metadata catalog and filter its `theme` array for `Hospitals` (case insensitive exact match).
2. Select all CSV distributions using their media type or CSV URL suffix.
3. Compare each resource's `modified`, `released`, URL and distribution modification metadata with its last successful version. Unchanged resources with an existing output are skipped before downloading. A changed URL is a new resource. Missing outputs are downloaded again. Resources without a modification timestamp are conservatively downloaded on each run.
4. Use a bounded thread pool to download and process files in parallel. Each worker streams CSV rows, so large files are not loaded into memory. Values remain strings, preserving leading zeros and original cell contents.
5. Normalize headers to ASCII snake_case. Apostrophes are removed, other punctuation and whitespace become underscores, and camel case is separated. Empty names become `column`; collisions receive numeric suffixes so all names remain unique. Original and normalized headers are recorded together in the run report.
6. Write to a temporary file and atomically replace the destination only after successful processing. Record row count, column count, SHA-256, local path, download time and version in SQLite. The main thread owns database writes.
7. Write a JSON run report and retain run and resource history in SQLite.

The incremental strategy relies on CMS keeping catalog modification metadata accurate. It compares against the last successful resource version, not simply the last attempted run, so failures do not cause missed updates. HTTP 408, 429 and transient server/network errors are retried with bounded exponential backoff. Stream interruption fails the resource without advancing its saved version. The next scheduled run retries it.

## Daily scheduling

Linux cron example (use absolute paths):

```cron
0 6 * * * /usr/bin/python3 /opt/cms/cms_hospitals.py --output /opt/cms/output --state /opt/cms/state/cms.sqlite >> /opt/cms/pipeline.log 2>&1
```

Windows Task Scheduler: create a daily task; set Program to the absolute path of `python.exe`, and Arguments to:

```text
"C:\cms\cms_hospitals.py" --output "C:\cms\output" --state "C:\cms\state\cms.sqlite" --workers 4
```

Set the working directory to `C:\cms`. Configure the scheduler to avoid overlapping runs. The script additionally uses an exclusive lock beside its state database. If a process is forcibly terminated, verify it is no longer running before removing its stale `.lock` file.

## Files and assumptions

- `cms_hospitals.py`: implementation.
- `requirements.txt`: notes that no external dependencies are needed.
- `sample_output/`: actual downloaded and transformed sample, with run reports when available.
- Output names use the dataset identifier and a URL hash to avoid filename collisions.
- Source CSVs are assumed to be comma delimited UTF-8, with optional BOM. Inconsistent row widths fail the resource rather than silently changing data.
- Historical outputs for removed or replaced URLs are retained; no destructive cleanup is performed.
- SQLite state and reports are operational metadata, not PHI. These CMS datasets are public provider data.

## Header example

`Patients' rating of the facility linear mean score` becomes `patients_rating_of_the_facility_linear_mean_score`.
