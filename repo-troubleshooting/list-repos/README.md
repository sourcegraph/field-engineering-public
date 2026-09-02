# `list-repos.py`

Export repository health and size metadata from a Sourcegraph instance to CSV

The script is meant for support and troubleshooting work: it streams the repo
list through Sourcegraph's GraphQL API, writes CSV files per run, and keeps
memory use flat on large instances

## Requirements

- Python 3.10 or newer
- Sourcegraph 5.2.0 or newer
- A Sourcegraph access token starting with `sgp_`
  - Some columns and repair actions need a site-admin token.
  Non-admin tokens can still list repositories, but admin-only
  CSV columns are left blank.

## Quick start

From this directory:

```sh
export SRC_ENDPOINT="https://sourcegraph.example.com"
export SRC_ACCESS_TOKEN="sgp_..."

python3 list-repos.py
```

The script also reads a local `.env` file when environment variables are not
set:

```sh
SRC_ENDPOINT=https://sourcegraph.example.com
SRC_ACCESS_TOKEN=sgp_...
```

Command-line credentials are supported, but environment variables or `.env` are
safer because they do not put the token in shell history:

```sh
python3 list-repos.py \
  --src-endpoint "https://sourcegraph.example.com" \
  --src-access-token "sgp_..."
```

## Common commands

```sh
# List every repository
python3 list-repos.py

# Smoke test against a small sample
python3 list-repos.py --limit 100

# Include repos whose latest index skipped files
python3 list-repos.py --skipped-files

# Explain skipped files for one repo and indexed revision
python3 list-repos.py --skipped-files-reason github.com/org/repo@main

# Append per-repo commit counts and cleanup metadata
python3 list-repos.py --count-commits

# Count commits for one repo only
python3 list-repos.py --count-commits github.com/org/repo@develop

# Count matches for a Sourcegraph search pattern in every repo
python3 list-repos.py --run-search 'TODO patternType:literal'

# Write size and index-ratio summary CSVs
python3 list-repos.py --statistics
```

### Failed repos

`--failed` narrows the run to repos with cloning errors, using server-side
filters (`failedFetch`, `corrupted`, `cloneStatus: NOT_CLONED`) instead of
scanning every repo. The same client-side error detection is then applied, so
the result matches `repos-with-cloning-errors.csv` from a full run, in a
fraction of the time on large instances

```sh
# List only repos with cloning errors
python3 list-repos.py --failed

# Trigger a fetch (updateMirrorRepository) on every failed repo
python3 list-repos.py --failed --fetch

# Reclone (recloneRepository) every failed repo
python3 list-repos.py --failed --reclone
```

### Repair mutations

Site admins can trigger repair mutations. `--fetch` and `--reclone` are
mutually exclusive, and without a `REPO` they require `--failed`:

```sh
# Fetch or reclone one repo, whether in an error state or not
python3 list-repos.py --fetch github.com/org/repo
python3 list-repos.py --reclone github.com/org/repo

# Reindex every cloned repo missing a search index
python3 list-repos.py --reindex

# Reindex one repo
python3 list-repos.py --reindex github.com/org/repo
```

Mutations are sent in aliased batches of 10 per GraphQL request, with up to
`--concurrency` requests in flight. Recloning is expensive on gitserver, so
lower `--concurrency` when recloning thousands of repos

Each repo's outcome lands in the `action` and `result` CSV columns, for example
`reclone triggered`, `reclone skipped` (another reclone is already in
progress), or `reclone failed` with the server's error message

## Output files

Each run writes to `list-repos-runs/<endpoint>/<timestamp>/`, so runs never
overwrite each other

| File                                | When written                                              |
| ----------------------------------- | --------------------------------------------------------- |
| `list-repos.log`                    | Every run                                                 |
| `repos.csv`                         | Every listing run                                         |
| `repos-with-cloning-errors.csv`     | When one or more repos have a cloning or corruption error |
| `repos-with-indexing-errors.csv`    | When one or more cloned repos are missing a search index  |
| `repos-with-skipped-files.csv`      | With `--skipped-files` and one or more skipped-file repos |
| `stats-*.csv`                       | With `--statistics`                                       |
| `skipped-files-reason-details.csv`  | With `--skipped-files-reason REPO[@REV]`                  |
| `skipped-files-reason-stats.csv`    | With `--skipped-files-reason REPO[@REV]`                  |

- Optional columns from `--count-commits`, `--run-search`, and the repair
  mutations are appended to the per-repo CSVs
- See [`CSV_SCHEMA.md`](CSV_SCHEMA.md) for the exact columns, types, and
  admin-only fields

## Operational notes

- `--count-commits` sends one extra GraphQL request per repository and can be
  slow on large monorepos
- The script writes progress and failures to `list-repos.log` and stderr

## Development notes

If you add, remove, rename, or reorder CSV columns in `list-repos.py`, update
the column tuples and regenerate the generated reference:

```sh
python3 list-repos.py --write-csv-schema
```

To refresh `schema.gql` from an instance for development:

```sh
npx -y get-graphql-schema \
  -h "Authorization=token $SRC_ACCESS_TOKEN" \
  "$SRC_ENDPOINT/.api/graphql" > schema.gql
```
