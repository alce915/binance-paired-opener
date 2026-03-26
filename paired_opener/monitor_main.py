from __future__ import annotations

import uvicorn

from paired_opener.config import settings
from paired_opener.monitor_api import app


def run() -> None:
    uvicorn.run(app, host=settings.monitor_api_host, port=settings.monitor_api_port)


if __name__ == "__main__":
    run()
