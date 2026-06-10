from database.session import SessionLocal
from scrapers.rss_news import RSSNewsIngestor
db = SessionLocal()
r = RSSNewsIngestor(db).ingest_feed(
    feed_url="https://news.google.com/rss/search?q=World+Bank&hl=en-KE&gl=KE&ceid=KE:en",
    source_label="Business Daily Africa"
)
print(r)