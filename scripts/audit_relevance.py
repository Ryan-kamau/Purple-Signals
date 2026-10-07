"""Replay RelevanceFilter over stored headlines. Dry-run by default; --apply deletes rejects."""
import sys
from collections import Counter

from database.session import SessionLocal
from intelligence.relevance_filter import RelevanceFilter
from models.headline_data import Headline
from scrapers.rss_news import DEFAULT_FEEDS

LOCAL_LABELS = {f["label"] for f in DEFAULT_FEEDS if f.get("region") == "kenya"}


def main(apply: bool) -> int:
    db, gate = SessionLocal(), RelevanceFilter()
    try:
        tiers, rejected_ids, rejected_samples = Counter(), [], []
        for h in db.query(Headline).yield_per(500):
            article = {
                "title": h.headline,
                "description": h.description,
                "impact_score": h.impact_score,
                "matched_keywords_count": h.matched_keywords_count,
                "categories": [c.strip() for c in (h.categories or "").split(",") if c.strip()],
            }
            r = gate.evaluate(article, is_local=h.source in LOCAL_LABELS)
            tiers[r.tier] += 1
            if not r.keep:
                rejected_ids.append(h.id)
                if len(rejected_samples) < 40:
                    rejected_samples.append((h.impact_score, h.source, h.headline))

        print("Tier counts:", dict(tiers))
        print("\nSample REJECTED (sorted by impact):")
        for impact, source, title in sorted(rejected_samples, reverse=True):
            print(f"  [{impact}] {source}: {title}")

        if apply and rejected_ids:
            for i in range(0, len(rejected_ids), 500):
                db.query(Headline).filter(Headline.id.in_(rejected_ids[i:i + 500])).delete(
                    synchronize_session=False
                )
            db.commit()
            print(f"\nDeleted {len(rejected_ids)} rows.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main("--apply" in sys.argv))