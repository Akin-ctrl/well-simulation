"""Check prose against the plain English standard.

Standard 4 in CODING_STANDARDS_COMMITMENT.md adopts the GOV.UK plain English
guidance. Some of it is objective and can be checked here. Tone and voice
cannot, and are left to review rather than faked with a rule.

Errors fail the build. Warnings are reported and do not.
"""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

EM_DASH = "—"

MAX_SENTENCE_WORDS = 25

# Extensions whose contents are prose or carry prose comments.
PROSE_SUFFIXES = frozenset({".md", ".py", ".ts", ".tsx", ".sql", ".yml", ".yaml"})

# Files allowed to contain an em dash, with the reason. The dashboard shows one
# as the placeholder for a reading that has no value, which is a display
# character rather than writing.
PLACEHOLDER = "missing value placeholder in the dashboard"

EM_DASH_ALLOWED: dict[str, str] = {
    "scripts/check_prose.py": "defines the character this check bans",
    "monorepo/apps/website/app/routes/_dashboard._index.tsx": PLACEHOLDER,
    "monorepo/apps/website/app/routes/_dashboard.analytics.tsx": PLACEHOLDER,
    "monorepo/apps/website/app/routes/_dashboard.wellheads.$wellheadId.tsx": (
        PLACEHOLDER
    ),
}

# Long words with a shorter everyday equivalent, from the GOV.UK list.
PREFERRED_WORDS: dict[str, str] = {
    "purchase": "buy",
    "assist": "help",
    "approximately": "about",
    "utilise": "use",
    "utilize": "use",
    "commence": "start",
    "terminate": "end",
    "endeavour": "try",
    "facilitate": "help",
    "prior to": "before",
    "subsequent to": "after",
    "in order to": "to",
    "with regard to": "about",
    "in the event that": "if",
    "at this point in time": "now",
    "a number of": "some",
    "the majority of": "most",
    "leverage": "use",
    "utilisation": "use",
}


@dataclass(frozen=True)
class Finding:
    """One problem found in one file."""

    path: str
    line: int
    rule: str
    message: str
    is_error: bool


def prose_files() -> list[Path]:
    """Return every file under version control whose contents may be prose.

    Includes files that are new and not yet committed, so a problem is caught
    before it lands rather than after.
    """
    git = shutil.which("git")
    if git is None:
        raise RuntimeError("git is required to list tracked files")

    # The executable is resolved from PATH and the arguments are fixed, so
    # there is no untrusted input in this call.
    result = subprocess.run(  # noqa: S603
        [git, "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return [
        REPO_ROOT / line
        for line in result.stdout.splitlines()
        if line and Path(line).suffix in PROSE_SUFFIXES
    ]


def check_em_dashes(relative: str, lines: list[str]) -> list[Finding]:
    """Report every em dash outside the allowed files."""
    if relative in EM_DASH_ALLOWED:
        return []

    return [
        Finding(
            path=relative,
            line=number,
            rule="em-dash",
            message="em dash; use a comma, colon, or a separate sentence",
            is_error=True,
        )
        for number, text in enumerate(lines, start=1)
        if EM_DASH in text
    ]


def prose_lines(relative: str, lines: list[str]) -> list[tuple[int, str]]:
    """Return the lines of a markdown file that are ordinary prose.

    Code blocks, tables, headings, and link lists are excluded: sentence length
    means nothing in them.
    """
    if not relative.endswith(".md"):
        return []

    kept: list[tuple[int, str]] = []
    in_code_block = False

    for number, text in enumerate(lines, start=1):
        stripped = text.strip()

        if stripped.startswith("```"):
            in_code_block = not in_code_block
            continue
        if in_code_block or not stripped:
            continue
        if stripped.startswith(("#", "|", ">", "-", "*", "1.")):
            continue

        kept.append((number, stripped))

    return kept


def check_sentence_length(relative: str, lines: list[str]) -> list[Finding]:
    """Warn about sentences longer than the standard allows."""
    findings: list[Finding] = []

    for number, text in prose_lines(relative, lines):
        for sentence in re.split(r"(?<=[.!?])\s+", text):
            words = len(sentence.split())
            if words > MAX_SENTENCE_WORDS:
                findings.append(
                    Finding(
                        path=relative,
                        line=number,
                        rule="long-sentence",
                        message=(
                            f"{words} words; split anything over {MAX_SENTENCE_WORDS}"
                        ),
                        is_error=False,
                    )
                )

    return findings


def check_word_choice(relative: str, lines: list[str]) -> list[Finding]:
    """Warn where a shorter everyday word would do."""
    findings: list[Finding] = []

    for number, text in prose_lines(relative, lines):
        # A word inside a code span is being quoted, not used. The rule about
        # plain word choice does not apply to it.
        lowered = re.sub(r"`[^`]*`", "", text).lower()
        for long_word, plain_word in PREFERRED_WORDS.items():
            if re.search(rf"\b{re.escape(long_word)}\b", lowered):
                findings.append(
                    Finding(
                        path=relative,
                        line=number,
                        rule="word-choice",
                        message=f'"{long_word}"; prefer "{plain_word}"',
                        is_error=False,
                    )
                )

    return findings


def check_file(path: Path) -> list[Finding]:
    """Run every check against one file."""
    relative = path.relative_to(REPO_ROOT).as_posix()

    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (UnicodeDecodeError, OSError):
        return []

    return [
        *check_em_dashes(relative, lines),
        *check_sentence_length(relative, lines),
        *check_word_choice(relative, lines),
    ]


def report(findings: list[Finding], show_warnings: bool) -> None:
    """Print findings grouped by severity."""
    errors = [f for f in findings if f.is_error]
    warnings = [f for f in findings if not f.is_error]

    for finding in errors:
        print(
            f"{finding.path}:{finding.line}: error: [{finding.rule}] {finding.message}"
        )

    if show_warnings:
        for finding in warnings:
            print(
                f"{finding.path}:{finding.line}: warning: "
                f"[{finding.rule}] {finding.message}"
            )

    print(f"\n{len(errors)} errors, {len(warnings)} warnings")


def main() -> int:
    """Check every prose file under version control."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--quiet", action="store_true", help="report errors only, hiding warnings"
    )
    args = parser.parse_args()

    findings: list[Finding] = []
    for path in prose_files():
        findings.extend(check_file(path))

    report(findings, show_warnings=not args.quiet)
    return 1 if any(f.is_error for f in findings) else 0


if __name__ == "__main__":
    raise SystemExit(main())
