from dataclasses import dataclass
from typing import Any

@dataclass
class FileCache:
    mtime: float = -1.0
    value: Any = None

_columns_cache = FileCache()
