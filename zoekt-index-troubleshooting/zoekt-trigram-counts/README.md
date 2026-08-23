# Count Zoekt trigrams in local files

`zoekt-trigram-counts.py` scans a local directory and counts each text file's
distinct three-character sequences using Zoekt-compatible UTF-8 decoding. It
helps identify files that Zoekt may skip because they contain too many unique
trigrams.

The script runs locally. It does not connect to or modify a Sourcegraph
instance.

## Requirements

- Python 3.10 or newer
- Git, when scanning Git worktrees

The script uses only the Python standard library.

## Usage

```sh
./zoekt-trigram-counts.py \
  --root ~/git \
  --output zoekt-trigram-counts.tsv
```

By default, it:

- scans `~/git`
- excludes Git-ignored files
- skips files containing null bytes as binary
- processes files in parallel using the available CPUs
- flags files with more than 20,000 distinct trigrams
- sorts results by descending trigram count
- skips later paths that share the same final 20 characters

Use `--dedupe-path-suffix-length 0` when every file must be included.

Run `./zoekt-trigram-counts.py --help` for all options.

## Output

The tab-separated output contains:

| Column | Description |
| --- | --- |
| `unique_trigrams` | Number of distinct three-character sequences |
| `would_skip_too_many_trigrams` | Whether the count exceeds the threshold |
| `byte_size` | File size in bytes |
| `path` | Local file path |

Progress, skipped-file counts, and a run summary are written to stderr.
