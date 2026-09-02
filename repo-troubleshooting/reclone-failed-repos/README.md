# `reclone_failed_repos.py`

Report every repository on a Sourcegraph instance whose last clone or fetch
failed, with its mirror diagnostics, and optionally fetch or reclone them all.

- Default: read-only. Lists the failed repositories and writes a CSV.
- `--fetch`: also queues a fetch of each repository's existing clone
  (`updateMirrorRepository`). Use this when the remote was flaky.
- `--reclone`: instead deletes each repository from gitserver disk, marks it
  as not cloned, and starts a fresh clone (`recloneRepository`). Use this when
  the on-disk copy is corrupt and a fetch will not fix it.

## Requirements

- Python 3.10 or newer (standard library only)
- A site-admin Sourcegraph access token, starting with `sgp_`

Works the same on Windows, Linux, and macOS. On Windows, use `python` or `py`
in place of `python3`.

## Quick start

From this directory:

```sh
# Linux / macOS
export SRC_ENDPOINT="https://sourcegraph.example.com"
export SRC_ACCESS_TOKEN="sgp_..."

# Windows PowerShell
$env:SRC_ENDPOINT = "https://sourcegraph.example.com"
$env:SRC_ACCESS_TOKEN = "sgp_..."

# Report failed repositories (read-only)
python3 reclone_failed_repos.py

# Fetch them
python3 reclone_failed_repos.py --fetch

# Reclone them
python3 reclone_failed_repos.py --reclone
```

The script also reads a `.env` file in the current directory when the
environment variables are not set:

```sh
SRC_ENDPOINT=https://sourcegraph.example.com
SRC_ACCESS_TOKEN=sgp_...
```

Credentials can also be passed as `--src-endpoint` and `--src-access-token`,
but environment variables or `.env` keep the token out of shell history.

## Options

```sh
# Only the first 5 failed repositories, for a small test run
python3 reclone_failed_repos.py --reclone --max-repos 5

# Mutations packed into each GraphQL request (default 10)
python3 reclone_failed_repos.py --fetch --batch-size 20

# Mutation requests sent at once (default 8); each recloneRepository call
# deletes the repo on every gitserver shard before returning, so higher
# values add load on gitserver, not just the frontend
python3 reclone_failed_repos.py --reclone --parallelism 2

# Failed repositories fetched per GraphQL query page when listing (default 100)
python3 reclone_failed_repos.py --list-repos-page-size 500
```

Repositories that already have a reclone in progress are skipped and
counted separately. The script exits non-zero if listing fails or if any
fetch or reclone fails for another reason.

## Output

Each run writes `yyyy-mm-dd-hh-mm-ss-failed-repos.csv` (local time) to the
current directory, one row per repository:

- `repo_name`, `sourcegraph_url`, `remote_url`, `gitserver_shard`, `size_mb`
- `cloned`, `clone_in_progress`, `is_corrupted`
- `last_successful_fetch`, `time_since_last_successful_fetch`
- `next_sync`, `time_until_next_sync`
- `update_schedule_due`, `time_until_update_schedule_due`,
  `update_schedule_interval_seconds`
- `update_queue_position`, `currently_updating`
- `last_error`, `last_sync_output`: collapsed to one line each
- `action`: `listed (dry run)`, `fetch triggered`, `fetch failed`,
  `reclone triggered`, `reclone skipped`, or `reclone failed`
- `result`: the fetch / reclone error or skip message, if any

Timestamps are RFC 3339 as returned by the API; the `time_*` columns are
relative to when the script ran (e.g. `3h 12m ago`, `in 45s`).

## Development

```sh
mise trust && mise install
mise run check
```
