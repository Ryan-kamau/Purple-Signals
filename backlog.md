
#change architecture in rss_news.py.....
In a stricter layered architecture this would probably be split into an RSSFetcher (infra), a NewsNormalizer (domain), and a HeadlineRepository (persistence)

Scalability: Chunked dedup queries and single-transaction bulk insert scale reasonably to large feeds. However, ingest_all_feeds processes feeds serially — for 9 default feeds each with network calls up to 15s timeout, a worst case is minutes of wall-clock time. This wouldn't scale well to hundreds of feeds without concurrency (threading, asyncio, or a task queue fan-out).

Expensive loops: _normalize_entries and _build_headline_dict are O(n) per feed with straightforward string ops — fine at typical RSS feed sizes (tens to low hundreds of entries)


in market_service resolve cacultion of volatility
Marketcalculationservice add trading calender

news_ingestor/rss_news....
Ingestion caps how meaningful the relevance fields can be. The RSS ingestor drops headlines with impact_score < 2. A plain "KPLC announces X" story with no macro keywords never reaches the table. The fix is a "keep if KPLC mentioned" bypass in the ingestor's filter step.

daily market features taable/.....
Or better, have your orchestration explicitly enforce the dependency:

market calculation succeeds
        ↓
news calculation runs

rather than relying purely on clock times.