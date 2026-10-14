
from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Column, Integer, String, DateTime, Numeric

# Inside your existing model class:

id = Column(Integer, primary_key=True, index=True)

ticker = Column(String(10), nullable=False, index=True)
company = Column(String(255), nullable=False, index=True)

report_date = Column(DateTime(timezone=True), nullable=False, index=True)

# Financial values
eps = Column(Numeric(12, 4))
pe_ratio = Column(Numeric(12, 4))
dividend_yield = Column(Numeric(12, 6))
revenue = Column(Numeric(20, 2))
debt_ratio = Column(Numeric(12, 6))
net_profit_margin = Column(Numeric(12, 6))

# Generate a fresh timestamp for each new object
timestamp = Column(
    DateTime(timezone=True),
    default=lambda: datetime.now(ZoneInfo("Africa/Nairobi"))
)
