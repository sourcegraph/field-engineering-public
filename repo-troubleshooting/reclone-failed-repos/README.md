# `reclone_failed_repos.py`

List every repository on a Sourcegraph instance whose last clone or fetch
failed, and optionally reclone them all.

Recloning a repository deletes it from gitserver disk, marks it as not cloned,
and starts a fresh clone. The script lists the failed repositories and exits
unless `--apply` is given.

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

# List failed repositories (read-only)
python3 reclone_failed_repos.py

# Reclone them
python3 reclone_failed_repos.py --apply
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
python3 reclone_failed_repos.py --apply --max-repos 5

# recloneRepository mutations packed into each GraphQL request (default 10)
python3 reclone_failed_repos.py --apply --reclone-batch-size 20

# Reclone requests sent at once (default 8); each recloneRepository call
# deletes the repo on every gitserver shard before returning, so higher
# values add load on gitserver, not just the frontend
python3 reclone_failed_repos.py --apply --reclone-parallelism 2

# Failed repositories fetched per GraphQL query page when listing (default 100)
python3 reclone_failed_repos.py --list-repos-page-size 500
```

Repositories that already have a reclone in progress are skipped and
counted separately. The script exits non-zero if listing fails or if any
reclone fails for another reason.

## Output

Each run writes `yyyy-mm-dd-hh-mm-ss-reclone-failed-repos.csv` (local time)
to the current directory, one row per repository:

- `repo_name`: e.g. `github.com/torvalds/linux`
- `action`: `listed (dry run)`, `reclone triggered`, `skipped`, or
  `reclone failed`
- `result`: last fetch error (dry run), or the reclone error / skip message

## Development

```sh
mise trust && mise install
mise run check
```
