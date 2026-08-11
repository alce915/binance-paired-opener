from __future__ import annotations

import uvicorn

from paired_opener.api import app
from paired_opener.config import settings
from paired_opener.single_instance import require_single_worker


def run() -> None:
    uvicorn.run(app, host=settings.api_host, port=settings.api_port, workers=require_single_worker())


if __name__ == "__main__":
    run()
