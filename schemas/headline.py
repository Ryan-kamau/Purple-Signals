from datetime import datetime

from pydantic import BaseModel, ConfigDict, computed_field, Field


class HeadlineResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    source: str
    headline: str
    description: str | None = None
    content: str | None = None
    published_at: datetime | None = None
    sentiment_score: float | None = None
    senntiment_label: str | None = None

    # Raw DB fields
    keywords_detected: str | None = None
    categories: str | None = None

    matched_keywords_count: int = 0
    impact_score: int = 0

    url: str
    timestamp: datetime

    @computed_field
    @property
    def keyword_list(self) -> list[str]:
        if not self.keywords_detected:
            return []

        return [
            keyword.strip()
            for keyword in self.keywords_detected.split(",")
            if keyword.strip()
        ]

    @computed_field
    @property
    def category_list(self) -> list[str]:
        if not self.categories:
            return []

        return [
            category.strip()
            for category in self.categories.split(",")
            if category.strip()
        ]

class IngestionResponse(BaseModel):
    """
    Response returned by NewsService ingestion operations.
    """

    success: bool

    endpoint: str

    fetched: int

    saved: int

    duplicates: int

    invalid: int

    errors: list[str]

    fallback_used: bool

    processed_at: datetime

    stored_ids: list[int] = Field(default_factory=list)

    error: str | None = None