import json
from pathlib import Path

_TICKERS_FILE = Path(__file__).parent / "tickers.json"


def load_tickers() -> list[str]:
    with open(_TICKERS_FILE) as f:
        return json.load(f)
