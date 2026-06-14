from database.session import SessionLocal
from scrapers.new_rss import RSSNewsIngestor
db = SessionLocal()
r = RSSNewsIngestor(db).ingest_feed(
    feed_url="https://www.capitalfm.co.ke/business/feed/",
    source_label="Business Daily Africa"
)
print(r)