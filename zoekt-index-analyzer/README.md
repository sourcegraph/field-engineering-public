# Analyze local files for Zoekt skip reasons

`zoekt-index-analyzer.py` analyzes one local file or every eligible file under a
directory. It writes file measurements and Zoekt skip checks to
`zoekt-index-analyzer.tsv` in the current working directory.

The analyzer runs locally. It does not connect to or modify a Sourcegraph
instance.

## Checks

The TSV reports these checks in Zoekt's evaluation order, using Sourcegraph's
defaults:

1. File size exceeds 1 MiB.
2. A non-empty file contains fewer than three bytes.
3. File content contains a null byte.
4. File content contains more than 20,000 distinct trigrams.

An empty file is not skipped. Values equal to either maximum are allowed. Each
TSV column reports whether its condition matches, even though Zoekt stops after
the first matching reason.

A trigram is a sequence of three adjacent UTF-8 characters. For example,
`abcd` contains the trigrams `abc` and `bcd`. The analyzer uses Zoekt-compatible
UTF-8 decoding and counts trigrams without loading the whole file into memory.

## Requirements

- Python 3.10 or newer
- Git, when analyzing a Git worktree

The analyzer uses only the Python standard library.

## Usage

Analyze the current working directory:

```sh
./zoekt-index-analyzer.py
```

Analyze one file or directory:

```sh
./zoekt-index-analyzer.py PATH
```

Directory scans use all available CPUs. In Git worktrees, tracked and
untracked files are included while Git-ignored files are excluded.

## Output

`zoekt-index-analyzer.tsv` contains:

| Column | Description |
| --- | --- |
| `path` | Local file path |
| `byte_size` | File size in bytes |
| `unique_trigrams` | Distinct three-character sequence count |
| `exceeds_maximum_size` | Whether size exceeds 1 MiB |
| `contains_too_few_trigrams` | Whether a non-empty file is under 3 bytes |
| `contains_binary_content` | Whether content contains a null byte |
| `contains_too_many_trigrams` | Whether the count exceeds 20,000 |

Progress, failures, skip-reason totals, and a run summary are written to
stderr.

## Sourcegraph workaround

First open a support ticket by emailing <support@sourcegraph.com>. If directed,
use targeted `search.largeFiles` glob patterns in Sourcegraph site configuration
to bypass the maximum file size and trigram checks. Avoid enabling large files
globally because they consume substantially more indexing and search resources.
