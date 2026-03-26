from __future__ import annotations

import uvicorn

from monitor_app.api import app
from monitor_app.config import settings


def run() -> None:
    uvicorn.run(app, host=settings.monitor_api_host, port=settings.monitor_api_port)


if __name__ == "__main__":
    run()