#!/usr/bin/env python3

from __future__ import annotations

import argparse
import concurrent.futures
import csv
import os
import stat
import subprocess
import sys
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Final, TextIO


DEFAULT_ROOT = Path("~/git")
DEFAULT_OUTPUT = Path("zoekt-trigram-counts.tsv")
DEFAULT_THRESHOLD = 20_000
DEFAULT_PATH_SUFFIX_LENGTH = 20
READ_CHUNK_SIZE = 1_048_576
RUNE_ERROR: Final = 0xFFFD


@dataclass(frozen=True)
class DiscoveredFiles:
    """Files found under the requested root."""

    git_worktrees: list[Path]
    plain_files: list[Path]


@dataclass(frozen=True)
class DecodedRune:
    """A rune decoded the same way Go's utf8.DecodeRune decodes bytes."""

    codepoint: int
    byte_count: int


@dataclass(frozen=True)
class CountedFile:
    """Zoekt-style unique trigram count for one text file."""

    path: str
    byte_size: int
    unique_trigrams: int


@dataclass(frozen=True)
class SkippedFile:
    """A file that could not or should not be counted."""

    path: str
    reason: str
    detail: str = ""


FileProcessingResult = CountedFile | SkippedFile


def parse_non_negative_integer(value_text: str) -> int:
    """Parse an argparse integer value that must be zero or greater."""
    try:
        value = int(value_text)
    except ValueError:
        message = f"{value_text!r} is not an integer"
        raise argparse.ArgumentTypeError(message) from None
    if value < 0:
        message = f"{value_text!r} must be zero or greater"
        raise argparse.ArgumentTypeError(message)
    return value


def parse_positive_integer(value_text: str) -> int:
    """Parse an argparse integer value that must be one or greater."""
    value = parse_non_negative_integer(value_text)
    if value == 0:
        message = f"{value_text!r} must be one or greater"
        raise argparse.ArgumentTypeError(message)
    return value


def parse_arguments(argument_values: Sequence[str]) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Count Sourcegraph Zoekt-style unique trigrams for local files. "
            "Git worktrees are listed with `git ls-files --cached --others "
            "--exclude-standard`, so .gitignore-covered files are excluded. "
            "Binary means Zoekt's null-byte binary check."
        )
    )
    parser.add_argument(
        "--root",
        type=Path,
        default=DEFAULT_ROOT,
        help="Root directory to scan. Default: %(default)s",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="TSV output path, or '-' for stdout. Default: %(default)s",
    )
    parser.add_argument(
        "--threshold",
        type=parse_non_negative_integer,
        default=DEFAULT_THRESHOLD,
        help="Zoekt TrigramMax threshold used for the would-skip column.",
    )
    parser.add_argument(
        "--dedupe-path-suffix-length",
        type=parse_non_negative_integer,
        default=DEFAULT_PATH_SUFFIX_LENGTH,
        help=(
            "Skip later files whose full path has the same final N characters. "
            "Use 0 to disable. Default: %(default)s"
        ),
    )
    parser.add_argument(
        "--workers",
        type=parse_positive_integer,
        default=max(1, os.cpu_count() or 1),
        help="Worker processes used while counting. Default: CPU count.",
    )
    parser.add_argument(
        "--sort",
        choices=("trigrams", "path"),
        default="trigrams",
        help="Sort TSV rows by descending trigram count or by path.",
    )
    parser.add_argument(
        "--progress-every",
        type=parse_non_negative_integer,
        default=1_000,
        help="Print progress every N files to stderr. Use 0 to disable.",
    )
    return parser.parse_args(argument_values)


def resolve_scan_root(root: Path) -> Path:
    """Expand and validate the scan root."""
    resolved_root = root.expanduser().resolve()
    if not resolved_root.exists():
        raise SystemExit(f"scan root does not exist: {resolved_root}")
    if not resolved_root.is_dir():
        raise SystemExit(f"scan root is not a directory: {resolved_root}")
    return resolved_root


def find_containing_git_worktree(root: Path) -> Path | None:
    """Return the Git worktree containing root, if root is inside one."""
    command = ["git", "-C", str(root), "rev-parse", "--show-toplevel"]
    try:
        completed_process = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return None

    if completed_process.returncode != 0:
        return None

    worktree_text = completed_process.stdout.decode("utf-8", errors="replace").strip()
    if not worktree_text:
        return None
    return Path(worktree_text).resolve()


def discover_files(root: Path) -> DiscoveredFiles:
    """Find Git worktrees and non-Git files under root."""
    containing_git_worktree = find_containing_git_worktree(root)
    if containing_git_worktree is not None and containing_git_worktree != root:
        return DiscoveredFiles(git_worktrees=[containing_git_worktree], plain_files=[])

    git_worktrees: list[Path] = []
    plain_files: list[Path] = []

    for current_directory_text, directory_names, file_names in os.walk(root):
        if ".git" in directory_names or ".git" in file_names:
            git_worktrees.append(Path(current_directory_text))
            directory_names.clear()
            continue

        directory_names[:] = [name for name in directory_names if name != ".git"]

        current_directory = Path(current_directory_text)
        for file_name in file_names:
            if file_name == ".git":
                continue
            plain_files.append(current_directory / file_name)

    return DiscoveredFiles(
        git_worktrees=sorted(git_worktrees),
        plain_files=sorted(plain_files),
    )


def iter_git_worktree_files(git_worktree: Path) -> Iterator[Path]:
    """Yield tracked and untracked, non-ignored files from one Git worktree."""
    command = [
        "git",
        "-C",
        str(git_worktree),
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
        "-z",
    ]
    try:
        completed_process = subprocess.run(
            command,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except FileNotFoundError:
        raise SystemExit("git was not found in PATH") from None

    if completed_process.returncode != 0:
        error_text = completed_process.stderr.decode("utf-8", errors="replace").strip()
        print(
            f"warning: could not list files in {git_worktree}: {error_text}",
            file=sys.stderr,
        )
        return

    for relative_path_bytes in completed_process.stdout.split(b"\0"):
        if not relative_path_bytes:
            continue
        yield git_worktree / Path(os.fsdecode(relative_path_bytes))


def collect_candidate_files(
    discovered_files: DiscoveredFiles, root: Path
) -> list[Path]:
    """Collect all files that should be considered for trigram counting."""
    candidate_files = list(discovered_files.plain_files)
    for git_worktree in discovered_files.git_worktrees:
        candidate_files.extend(iter_git_worktree_files(git_worktree))
    return sorted(
        (
            candidate_file
            for candidate_file in candidate_files
            if is_relative_to(candidate_file, root)
        ),
        key=path_sort_text,
    )


def is_relative_to(path: Path, root: Path) -> bool:
    """Return whether path is under root without following symlinks."""
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def path_sort_text(path: Path) -> str:
    """Return a stable text form for path sorting and suffix comparison."""
    return path.as_posix()


def dedupe_by_path_suffix(
    candidate_files: list[Path], path_suffix_length: int
) -> tuple[list[Path], int]:
    """Skip later paths with the same final N characters."""
    if path_suffix_length == 0:
        return candidate_files, 0

    seen_path_suffixes: set[str] = set()
    kept_files: list[Path] = []
    skipped_count = 0

    for file_path in candidate_files:
        path_text = path_sort_text(file_path)
        path_suffix = path_text[-path_suffix_length:]
        if path_suffix in seen_path_suffixes:
            skipped_count += 1
            continue
        seen_path_suffixes.add(path_suffix)
        kept_files.append(file_path)

    return kept_files, skipped_count


def exclude_output_file(candidate_files: list[Path], output_path: Path) -> list[Path]:
    """Do not count a pre-existing TSV from an earlier run."""
    if str(output_path) == "-":
        return candidate_files

    expanded_output_path = output_path.expanduser()
    if not expanded_output_path.is_absolute():
        expanded_output_path = Path.cwd() / expanded_output_path
    output_path_text = path_sort_text(expanded_output_path.resolve(strict=False))
    return [
        candidate_file
        for candidate_file in candidate_files
        if path_sort_text(candidate_file) != output_path_text
    ]


def decode_go_utf8_prefix(
    content: bytes, start: int, end: int, *, final: bool
) -> DecodedRune | None:
    """Decode one rune like Go's utf8.DecodeRune.

    Returns None only when more bytes are needed from the next chunk.
    """
    first_byte = content[start]
    if first_byte < 0x80:
        return DecodedRune(first_byte, 1)
    if 0xC2 <= first_byte <= 0xDF:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=2,
            second_byte_minimum=0x80,
            second_byte_maximum=0xBF,
        )
    if first_byte == 0xE0:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=3,
            second_byte_minimum=0xA0,
            second_byte_maximum=0xBF,
        )
    if 0xE1 <= first_byte <= 0xEC or 0xEE <= first_byte <= 0xEF:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=3,
            second_byte_minimum=0x80,
            second_byte_maximum=0xBF,
        )
    if first_byte == 0xED:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=3,
            second_byte_minimum=0x80,
            second_byte_maximum=0x9F,
        )
    if first_byte == 0xF0:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=4,
            second_byte_minimum=0x90,
            second_byte_maximum=0xBF,
        )
    if 0xF1 <= first_byte <= 0xF3:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=4,
            second_byte_minimum=0x80,
            second_byte_maximum=0xBF,
        )
    if first_byte == 0xF4:
        return decode_go_multibyte_utf8(
            content,
            start,
            end,
            final=final,
            byte_count=4,
            second_byte_minimum=0x80,
            second_byte_maximum=0x8F,
        )
    return DecodedRune(RUNE_ERROR, 1)


def decode_go_multibyte_utf8(
    content: bytes,
    start: int,
    end: int,
    *,
    final: bool,
    byte_count: int,
    second_byte_minimum: int,
    second_byte_maximum: int,
) -> DecodedRune | None:
    """Decode a valid multi-byte UTF-8 prefix or Go's replacement rune."""
    available_bytes = end - start
    if available_bytes < 2:
        return DecodedRune(RUNE_ERROR, 1) if final else None

    second_byte = content[start + 1]
    if second_byte < second_byte_minimum or second_byte > second_byte_maximum:
        return DecodedRune(RUNE_ERROR, 1)

    for byte_offset in range(2, byte_count):
        if available_bytes <= byte_offset:
            return DecodedRune(RUNE_ERROR, 1) if final else None
        continuation_byte = content[start + byte_offset]
        if continuation_byte < 0x80 or continuation_byte > 0xBF:
            return DecodedRune(RUNE_ERROR, 1)

    first_byte = content[start]
    if byte_count == 2:
        codepoint = ((first_byte & 0x1F) << 6) | (second_byte & 0x3F)
    elif byte_count == 3:
        third_byte = content[start + 2]
        codepoint = (
            ((first_byte & 0x0F) << 12)
            | ((second_byte & 0x3F) << 6)
            | (third_byte & 0x3F)
        )
    else:
        third_byte = content[start + 2]
        fourth_byte = content[start + 3]
        codepoint = (
            ((first_byte & 0x07) << 18)
            | ((second_byte & 0x3F) << 12)
            | ((third_byte & 0x3F) << 6)
            | (fourth_byte & 0x3F)
        )
    return DecodedRune(codepoint, byte_count)


def add_trigram_codepoint(
    trigrams: set[int], first_previous: int, second_previous: int, codepoint: int
) -> tuple[int, int]:
    """Shift one rune into the current trigram window."""
    if first_previous != 0:
        trigrams.add((first_previous << 42) | (second_previous << 21) | codepoint)
    return second_previous, codepoint


def count_buffer_trigrams(
    content: bytes,
    *,
    final: bool,
    trigrams: set[int],
    first_previous: int,
    second_previous: int,
) -> tuple[bytes, int, int]:
    """Count all complete runes in content and return leftover bytes."""
    offset = 0
    content_length = len(content)
    while offset < content_length:
        decoded_rune = decode_go_utf8_prefix(
            content, offset, content_length, final=final
        )
        if decoded_rune is None:
            break
        first_previous, second_previous = add_trigram_codepoint(
            trigrams,
            first_previous,
            second_previous,
            decoded_rune.codepoint,
        )
        offset += decoded_rune.byte_count

    return content[offset:], first_previous, second_previous


def count_bytes_trigrams(content: bytes) -> int:
    """Count unique Zoekt-style three-rune trigrams in a byte string."""
    trigrams: set[int] = set()
    leftover_bytes, first_previous, second_previous = count_buffer_trigrams(
        content,
        final=True,
        trigrams=trigrams,
        first_previous=0,
        second_previous=0,
    )
    if leftover_bytes:
        raise RuntimeError("final trigram count left undecoded bytes")
    return len(trigrams)


def process_regular_file(file_path: Path, byte_size: int) -> FileProcessingResult:
    """Count trigrams in a regular file without loading it all into memory."""
    trigrams: set[int] = set()
    leftover_bytes = b""
    first_previous = 0
    second_previous = 0

    try:
        with file_path.open("rb") as file_handle:
            while True:
                chunk = file_handle.read(READ_CHUNK_SIZE)
                if not chunk:
                    break
                if b"\0" in chunk:
                    return SkippedFile(str(file_path), "binary")

                content = leftover_bytes + chunk
                leftover_bytes, first_previous, second_previous = count_buffer_trigrams(
                    content,
                    final=False,
                    trigrams=trigrams,
                    first_previous=first_previous,
                    second_previous=second_previous,
                )

        if leftover_bytes:
            leftover_bytes, first_previous, second_previous = count_buffer_trigrams(
                leftover_bytes,
                final=True,
                trigrams=trigrams,
                first_previous=first_previous,
                second_previous=second_previous,
            )
            if leftover_bytes:
                return SkippedFile(str(file_path), "decode_error")
    except OSError as error:
        return SkippedFile(str(file_path), "read_error", str(error))

    return CountedFile(str(file_path), byte_size, len(trigrams))


def process_file(file_path_text: str) -> FileProcessingResult:
    """Count one regular file or Git-style symlink blob."""
    file_path = Path(file_path_text)
    try:
        file_status = file_path.lstat()
    except OSError as error:
        return SkippedFile(file_path_text, "stat_error", str(error))

    if stat.S_ISLNK(file_status.st_mode):
        try:
            content = os.fsencode(os.readlink(file_path))
        except OSError as error:
            return SkippedFile(file_path_text, "readlink_error", str(error))
        return CountedFile(file_path_text, len(content), count_bytes_trigrams(content))

    if not stat.S_ISREG(file_status.st_mode):
        return SkippedFile(file_path_text, "not_regular_file")

    return process_regular_file(file_path, file_status.st_size)


def process_files(
    candidate_files: list[Path], *, workers: int, progress_every: int
) -> tuple[list[CountedFile], dict[str, int], list[SkippedFile]]:
    """Count files and summarize skipped files."""
    counted_files: list[CountedFile] = []
    skipped_counts: dict[str, int] = {}
    skipped_examples: list[SkippedFile] = []
    path_texts = [str(file_path) for file_path in candidate_files]

    if workers == 1:
        processing_results = map(process_file, path_texts)
        counted_files, skipped_counts, skipped_examples = collect_processing_results(
            processing_results,
            total_files=len(path_texts),
            progress_every=progress_every,
        )
    else:
        with concurrent.futures.ProcessPoolExecutor(max_workers=workers) as executor:
            processing_results = executor.map(process_file, path_texts, chunksize=16)
            counted_files, skipped_counts, skipped_examples = (
                collect_processing_results(
                    processing_results,
                    total_files=len(path_texts),
                    progress_every=progress_every,
                )
            )

    return counted_files, skipped_counts, skipped_examples


def collect_processing_results(
    processing_results: Iterator[FileProcessingResult],
    *,
    total_files: int,
    progress_every: int,
) -> tuple[list[CountedFile], dict[str, int], list[SkippedFile]]:
    """Collect worker results and print progress."""
    counted_files: list[CountedFile] = []
    skipped_counts: dict[str, int] = {}
    skipped_examples: list[SkippedFile] = []

    for processed_count, processing_result in enumerate(processing_results, start=1):
        if isinstance(processing_result, CountedFile):
            counted_files.append(processing_result)
        else:
            skipped_counts[processing_result.reason] = (
                skipped_counts.get(processing_result.reason, 0) + 1
            )
            if len(skipped_examples) < 10:
                skipped_examples.append(processing_result)

        if progress_every and processed_count % progress_every == 0:
            print(
                f"processed {processed_count:,}/{total_files:,} files...",
                file=sys.stderr,
            )

    return counted_files, skipped_counts, skipped_examples


def sort_counted_files(counted_files: list[CountedFile], sort: str) -> None:
    """Sort counted files in place."""
    if sort == "path":
        counted_files.sort(key=lambda counted_file: counted_file.path)
    else:
        counted_files.sort(
            key=lambda counted_file: (-counted_file.unique_trigrams, counted_file.path)
        )


def write_tab_separated_values(
    output_path: Path,
    counted_files: list[CountedFile],
    *,
    threshold: int,
) -> None:
    """Write counted files to TSV."""
    if str(output_path) == "-":
        write_tab_separated_values_to_file(
            sys.stdout, counted_files, threshold=threshold
        )
        return

    resolved_output_path = output_path.expanduser()
    resolved_output_path.parent.mkdir(parents=True, exist_ok=True)
    with resolved_output_path.open("w", encoding="utf-8", newline="") as file_handle:
        write_tab_separated_values_to_file(
            file_handle, counted_files, threshold=threshold
        )


def write_tab_separated_values_to_file(
    file_handle: TextIO,
    counted_files: list[CountedFile],
    *,
    threshold: int,
) -> None:
    """Write TSV rows to an open file handle."""
    writer = csv.writer(file_handle, delimiter="\t", lineterminator="\n")
    writer.writerow(
        [
            "unique_trigrams",
            "would_skip_too_many_trigrams",
            "byte_size",
            "path",
        ]
    )
    for counted_file in counted_files:
        writer.writerow(
            [
                counted_file.unique_trigrams,
                str(counted_file.unique_trigrams > threshold).lower(),
                counted_file.byte_size,
                counted_file.path,
            ]
        )


def print_summary(
    *,
    output_path: Path,
    discovered_files: DiscoveredFiles,
    candidate_count: int,
    suffix_duplicate_count: int,
    counted_files: list[CountedFile],
    skipped_counts: dict[str, int],
    skipped_examples: list[SkippedFile],
    threshold: int,
) -> None:
    """Print a short run summary to stderr."""
    too_many_trigrams_count = sum(
        1 for counted_file in counted_files if counted_file.unique_trigrams > threshold
    )
    print(f"Git worktrees: {len(discovered_files.git_worktrees):,}", file=sys.stderr)
    print(
        f"Plain non-Git files: {len(discovered_files.plain_files):,}", file=sys.stderr
    )
    print(
        f"Candidate files before suffix de-dupe: {candidate_count:,}", file=sys.stderr
    )
    print(
        f"Skipped duplicate path suffixes: {suffix_duplicate_count:,}", file=sys.stderr
    )
    print(f"Counted text files: {len(counted_files):,}", file=sys.stderr)
    print(
        f"Files over {threshold:,} unique trigrams: {too_many_trigrams_count:,}",
        file=sys.stderr,
    )
    for reason, count in sorted(skipped_counts.items()):
        print(f"Skipped {reason}: {count:,}", file=sys.stderr)
    for skipped_file in skipped_examples:
        detail = f": {skipped_file.detail}" if skipped_file.detail else ""
        print(
            f"example skipped {skipped_file.reason}: {skipped_file.path}{detail}",
            file=sys.stderr,
        )
    if str(output_path) != "-":
        print(f"Wrote {output_path.expanduser()}", file=sys.stderr)


def run(parsed_arguments: argparse.Namespace) -> None:
    """Run the trigram count command."""
    root = resolve_scan_root(parsed_arguments.root)
    discovered_files = discover_files(root)
    candidate_files = collect_candidate_files(discovered_files, root)
    candidate_files = exclude_output_file(candidate_files, parsed_arguments.output)
    candidate_count = len(candidate_files)
    candidate_files, suffix_duplicate_count = dedupe_by_path_suffix(
        candidate_files,
        parsed_arguments.dedupe_path_suffix_length,
    )
    counted_files, skipped_counts, skipped_examples = process_files(
        candidate_files,
        workers=parsed_arguments.workers,
        progress_every=parsed_arguments.progress_every,
    )
    sort_counted_files(counted_files, parsed_arguments.sort)
    write_tab_separated_values(
        parsed_arguments.output,
        counted_files,
        threshold=parsed_arguments.threshold,
    )
    print_summary(
        output_path=parsed_arguments.output,
        discovered_files=discovered_files,
        candidate_count=candidate_count,
        suffix_duplicate_count=suffix_duplicate_count,
        counted_files=counted_files,
        skipped_counts=skipped_counts,
        skipped_examples=skipped_examples,
        threshold=parsed_arguments.threshold,
    )


def main(argument_values: Sequence[str] | None = None) -> None:
    """Command-line entry point."""
    run(parse_arguments(sys.argv[1:] if argument_values is None else argument_values))


if __name__ == "__main__":
    main()
