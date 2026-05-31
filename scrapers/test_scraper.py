import json
import logging

from scrapers.cbk_scraper import (
    scrape_forex_rates,
    scrape_central_bank_rate,
    scrape_mpc_releases,
    run_cbk_scraper,
)


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def print_section(title: str):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def test_forex():
    print_section("FOREX RATES")

    data = scrape_forex_rates()

    print(json.dumps(data, indent=4))


def test_cbr():
    print_section("CENTRAL BANK RATE")

    data = scrape_central_bank_rate()

    print(json.dumps(data, indent=4))


def test_mpc():
    print_section("MPC RELEASES")

    data = scrape_mpc_releases(limit=3)

    print(json.dumps(data, indent=4))


def test_all():
    print_section("FULL CBK SCRAPER OUTPUT")

    data = run_cbk_scraper()

    print(json.dumps(data, indent=4))


if __name__ == "__main__":

    # Uncomment what you want to test

    # test_forex()

    # test_cbr()

    # test_mpc()

    test_all()