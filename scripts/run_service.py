from __future__ import annotations

import argparse
import os
import sys
import time
import traceback
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run paired opener service in a detached Python process.")
    parser.add_argument("--project-root", required=True)
    parser.add_argument("--site-packages", required=True)
    parser.add_argument("--runtime-log", required=True)
    parser.add_argument("--host", required=True)
    parser.add_argument("--port", type=int, required=True)
    return parser.parse_args()


def configure_runtime(project_root: Path, site_packages: Path, runtime_log: Path) -> None:
    if site_packages.exists():
        sys.path.insert(0, str(site_packages))
    sys.path.insert(0, str(project_root))
    os.chdir(project_root)

    runtime_log.parent.mkdir(parents=True, exist_ok=True)
    stream = runtime_log.open("a", encoding="utf-8", buffering=1)
    sys.stdout = stream
    sys.stderr = stream

    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Paired opener service bootstrap")
    print(f"project_root={project_root}")


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    site_packages = Path(args.site_packages).resolve()
    runtime_log = Path(args.runtime_log).resolve()

    configure_runtime(project_root, site_packages, runtime_log)

    try:
        import uvicorn

        uvicorn.run("paired_opener.api:app", host=args.host, port=args.port)
        return 0
    except Exception:
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
