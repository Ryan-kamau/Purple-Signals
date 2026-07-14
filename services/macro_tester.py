"""
Integration test for MacroService.

Exercises the real KNBSExtractor against a live KNBS PDF and persists into
the real MySQL database via the project's SQLAlchemy session. No mocks,
no SQLite, no fixtures standing in for external systems.

Assumes a `db_session` fixture already exists in conftest.py, yielding a
live SQLAlchemy Session against the project's MySQL database. Adjust the
fixture name below if yours differs.
"""

import pytest
from sqlalchemy.orm import sessionmaker
from database.session import SessionLocal
from pprint import pprint

from models.macro_data import MacroData
from services.macro_service import MacroService

KNBS_PDF_URL = "https://www.knbs.or.ke/wp-content/uploads/2026/06/Kenya-Leading-Economic-Indicators-April-2026.pdf"

@pytest.fixture
def service(db_session):
    return MacroService(db_session)


def test_refresh_macro_data_full_pipeline(db_session, service):
    result = service.refresh_macro_data(KNBS_PDF_URL)

    assert result["status"] in ("success", "partial_success")
    assert result["rows_saved"] >= 0
    assert result["report_date"]
    assert result["results"]

    expected_extractors = {"inflation", "exchange", "cbr", "fuel"}
    assert expected_extractors.issubset(result["results"].keys())

    for key, sub_result in result["results"].items():
        assert "status" in sub_result
        assert "data" in sub_result
        if sub_result["status"] == "success":
            assert sub_result["data"], f"{key} succeeded but returned no rows"


def test_records_are_persisted_to_mysql(db_session, service):
    before_count = db_session.query(MacroData).count()

    result = service.refresh_macro_data(KNBS_PDF_URL)

    after_count = db_session.query(MacroData).count()

    assert result["status"] in ("success", "partial_success")
    assert after_count >= before_count
    assert after_count > 0

    saved_rows = db_session.query(MacroData).all()
    assert any(row.month and row.year for row in saved_rows)


def test_running_twice_does_not_create_duplicates(db_session, service):
    first_result = service.refresh_macro_data(KNBS_PDF_URL)
    count_after_first = db_session.query(MacroData).count()

    second_result = service.refresh_macro_data(KNBS_PDF_URL)
    count_after_second = db_session.query(MacroData).count()

    assert first_result["status"] in ("success", "partial_success")
    assert second_result["status"] in ("success", "partial_success")

    assert count_after_second == count_after_first

    for sub_result in second_result["results"].values():
        assert sub_result.get("rows_inserted", 0) == 0


def test_individual_refresh_methods_run_against_real_data(db_session, service):
    inflation_result = service.refresh_inflation(KNBS_PDF_URL)

    assert inflation_result["status"] in ("success", "failed")
    if inflation_result["status"] == "success":
        assert inflation_result["report_date"]
        assert inflation_result["rows_saved"] >= 0
        for record in inflation_result["data"]:
            assert record["month"]
            assert record["year"]


Session = SessionLocal

def main():
    session = Session()

    try:
        service = MacroService(session)
        result = service.refresh_macro_data(KNBS_PDF_URL)
        pprint(result)
    finally:
        session.close()


if __name__ == "__main__":
    main()