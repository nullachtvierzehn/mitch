from typing import Optional, Self
from collections import namedtuple

class CompositeId(namedtuple('CompositeId', ['repository_id','migration_id'])):
    def __str__(self) -> str:
        return f"{self.repository_id}::{self.migration_id}"
    
    @classmethod
    def from_string(cls, string: str, prefix: Optional[str] = None) -> Self:
        parts = tuple(string.split("::", 1))
        if len(parts) == 2:
            return cls.from_tuple(parts)
        elif len(parts) == 1 and prefix:
            return cls(prefix, parts[0])
        else:
            raise ValueError(f"Invalid composite id: {string}")

    @classmethod
    def from_tuple(cls, tuple: tuple[str, str]) -> Self:
        return cls(*tuple)
    
    @classmethod
    def from_string_or_tuple(cls, string_or_tuple: str | tuple[str] | tuple[str, str], prefix: Optional[str] = None) -> Self:
        if isinstance(string_or_tuple, tuple):
            if len(string_or_tuple) == 2:
                return cls.from_tuple(string_or_tuple)
            else:
                return cls.from_string(string_or_tuple[0], prefix=prefix)
        else:
            return cls.from_string(string_or_tuple, prefix=prefix)