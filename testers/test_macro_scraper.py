
from scrapers.macro_scraper import KNBSExtractor


sample_url = (
     "https://www.knbs.or.ke/wp-content/uploads/2026/09/Kenya-Leading-Economic-Indicators-July-2026.pdf"
 )
knbs = KNBSExtractor()

cbr_results = knbs.get_cbr(sample_url)


inflation_results = knbs.get_inflation(sample_url)
exchange_results = knbs.get_exchange_rates(sample_url)
fuel_results = knbs.get_fuels(sample_url)

print(cbr_results)
print(inflation_results)
print(exchange_results)
print(fuel_results)

knbs.close()