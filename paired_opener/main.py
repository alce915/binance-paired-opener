from __future__ import annotations

import uvicorn

from paired_opener.api import app
from paired_opener.config import settings


def run() -> None:
    uvicorn.run(app, host=settings.api_host, port=settings.api_port)


if __name__ == "__main__":
    run()
