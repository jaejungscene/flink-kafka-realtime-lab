from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote

ROOT = Path(__file__).resolve().parents[1]
MARKDOWN_LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
EXTERNAL_PREFIXES = ("http://", "https://", "mailto:", "#")


def local_link_targets(markdown_path: Path) -> list[tuple[str, Path]]:
    targets: list[tuple[str, Path]] = []
    content = markdown_path.read_text(encoding="utf-8")
    for raw_target in MARKDOWN_LINK.findall(content):
        target = raw_target.strip().split(maxsplit=1)[0].strip("<>")
        if not target or target.startswith(EXTERNAL_PREFIXES):
            continue
        relative_path = unquote(target.split("#", 1)[0])
        if relative_path:
            targets.append((target, (markdown_path.parent / relative_path).resolve()))
    return targets


def main() -> None:
    markdown_files = [ROOT / "README.md", *sorted((ROOT / "docs").glob("*.md"))]
    failures: list[str] = []

    for markdown_path in markdown_files:
        for target, resolved_path in local_link_targets(markdown_path):
            if not resolved_path.is_relative_to(ROOT) or not resolved_path.exists():
                failures.append(
                    f"{markdown_path.relative_to(ROOT)}: missing local link target: {target}"
                )

    if failures:
        raise SystemExit("\n".join(failures))
    print(f"markdown links ok: files={len(markdown_files)}")


if __name__ == "__main__":
    main()
