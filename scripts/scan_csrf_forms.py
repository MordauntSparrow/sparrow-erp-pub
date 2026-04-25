"""Dev utility: print POST <form> blocks under app/ missing SeaSurf _csrf_token / csrf_token()."""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "app"
CSRF = re.compile(r"name\s*=\s*['\"]_csrf_token['\"]|csrf_token\s*\(\)")


def main() -> int:
    issues = []
    for p in ROOT.rglob("*.html"):
        text = p.read_text(encoding="utf-8", errors="replace")
        i = 0
        while True:
            m = re.search(r"(?is)<form\b([^>]*)>", text[i:])
            if not m:
                break
            start = i + m.start()
            attrs = m.group(1)
            i = i + m.end()
            mm = re.search(r"\bmethod\s*=\s*['\"]([^'\"]*)['\"]", attrs, re.I)
            if not mm or mm.group(1).lower() != "post":
                continue
            em = re.search(r"(?is)</form\s*>", text[i:])
            if not em:
                continue
            inner = text[i : i + em.start()]
            if CSRF.search(inner) or CSRF.search(attrs):
                continue
            am = re.search(r"\baction\s*=\s*['\"]([^'\"]+)['\"]", attrs, re.I)
            act = (am.group(1) or "").strip() if am else ""
            if act.startswith("http://") or act.startswith("https://"):
                continue
            line = text[:start].count("\n") + 1
            issues.append((p.relative_to(ROOT).as_posix(), line))
    for path, line in sorted(issues):
        print(f"{path}:{line}")
    return 0 if not issues else 1


if __name__ == "__main__":
    sys.exit(main())
