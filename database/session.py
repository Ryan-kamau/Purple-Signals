from sqlalchemy.orm import sessionmaker
from .connection import engine
from database.base import Base  

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def init_db() -> None:
    """
    Create all tables that don't exist yet.
 
    Call once at application startup (see app/main.py).
    For production, replace with Alembic migrations.
    """
    Base.metadata.create_all(bind=engine)
 
 
def get_db():
    """
    FastAPI dependency — yields a SQLAlchemy session per request
    and guarantees the session is closed even on exception.
 
    Usage:
        @router.get("/market-data")
        def endpoint(db: Session = Depends(get_db)):
            ...
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()