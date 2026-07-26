"""
Common Schema Envelopes.
Houses generic wrappers (such as pagination records envelope structures).
"""

from typing import Generic, TypeVar, List, Optional
from pydantic import BaseModel

T = TypeVar("T")

class PaginationResponse(BaseModel, Generic[T]):
    """
    Generic paginated response wrapper envelope containing items, next page token, and limit constraints.
    """
    items: List[T]
    next_cursor: Optional[str] = None
    limit: int
