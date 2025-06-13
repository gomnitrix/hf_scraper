from typing import Any

def safe_int_parse(value: Any, default: int = 0) -> int:
    """Safely parse a value to integer, handling None and invalid cases."""
    if value is None or value == "None" or value == "":
        return default
    try:
        return int(float(str(value)))
    except (ValueError, TypeError):
        return default