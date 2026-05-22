from database.connection import engine
from database.base import Base
from models.market_data import MarketData
from models.market_data import MarketData
from models.headline_data import Headline
from models.fundamental_data import Fundamentals
from models.alert_data import Alert
from models.macro_data import MacroData
from models.sentiments_data import SentimentScore
from models.analytics_data import AnalysisResult


def _init_db():
    Base.metadata.create_all(bind=engine)
    print("Database initialized successfully")

def main():
    _init_db()

if __name__ == "__main__":
    main()