import logging
from pathlib import Path
import requests


def discord_notify(webhook: str, path: Path, msg: str = "") -> None:
    if not webhook or not path.exists():
        return
    try:
        with path.open("rb") as fh:
            r = requests.post(
                webhook,
                data={"content": msg},
                files={"file": (path.name, fh, "image/png")},
            )
            r.raise_for_status()
            logging.info("discord ok")
    except Exception as e:
        logging.error("discord fail %s", e)
