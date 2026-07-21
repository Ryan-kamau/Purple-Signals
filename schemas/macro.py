# schemas/macro.py

from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any
class IngestionResponse(BaseModel):
    """
    Response returned by MacroData ingestion operations.
    """

    success: bool

    report_date: datetime

    saved: int

    results: Dict[str, Dict[str, Any]]

    errors: list[str]

    fallback_used: bool

"""
Response schema for the read-only /macroeconomics router.

MacroQueryService already returns fully-formed, structured dictionaries
(status, message, counts, data, etc.). This schema exists purely for
OpenAPI documentation and light typing — it intentionally allows extra
fields (`extra="allow"`) so the router can return the service's dict
as-is without FastAPI silently dropping any keys the service includes
that aren't explicitly modeled here.
"""

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict


class MacroQueryResponse(BaseModel):
    """
    Generic envelope mirroring MacroQueryService's response shape.

    Fields are all optional since not every service method populates
    every key (e.g. snapshot/lookup responses may omit pagination
    fields such as count/total/limit/offset).
    """

    model_config = ConfigDict(extra="allow")

    status: Optional[str] = None
    message: Optional[str] = None
    count: Optional[int] = None
    total: Optional[int] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    data: Optional[Any] = None
