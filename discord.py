import requests


def post_image(path: str, webhook_url: str) -> None:
    if not webhook_url:
        return
    with open(path, "rb") as fh:
        files = {"file": fh}
        data = {"content": "BTC-USD TPO + CVA (last 7d)"}
        requests.post(webhook_url, files=files, data=data, timeout=15)


if __name__ == "__main__":
    import sys
    post_image(sys.argv[1], sys.argv[2])
