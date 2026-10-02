from database.session import SessionLocal
from scrapers.rss_news import RSSNewsIngestor
db = SessionLocal()
r = RSSNewsIngestor(db).ingest_feed(
    feed_url="https://thekenyatimes.com/feed/",
    source_label="Kenya Times"
)
print(r)