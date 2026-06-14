import io
import pdfplumber
import requests

# 1. Fetch the lightweight KPLC financial summary PDF directly into memory
pdf_url = "https://www.nse.co.ke/wp-content/uploads/The-Kenya-Power-Lighting-Company-Plc-Audited-Financial-Results-for-the-Year-Ended-30-Jun-2025.pdf"
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(pdf_url, headers=headers)
pdf_file = io.BytesIO(response.content)

eps_value = None

# 2. Extract text tables from the PDF pages
with pdfplumber.open(pdf_file) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        
        # Look for the page containing the income statement
        if "Earnings per share" in text or "Basic and diluted earnings" in text:
            lines = text.split("\n")
            for line in lines:
                if "Basic and diluted earnings per share" in line:
                    # Example line string: "Basic and diluted earnings per share (Kshs) 12.54 15.41"
                    parts = line.split()
                    # The closest number following the string is usually the latest year's EPS
                    for part in parts:
                        try:
                            # Clean punctuation and try parsing as float
                            val = float(part.replace(",", ""))
                            if val > 0:  # Adjust logic based on expected range
                                eps_value = val
                                break
                        except ValueError:
                            continue

if eps_value:
    print(f"Successfully Extracted KPLC EPS: KES {eps_value}")
    
    # 3. Simulate or pull the live market price of KPLC from the stock exchange
    # (Assuming a live market price of KES 15.45 as an example baseline)
    kplc_market_price = 15.45  
    
    # 4. Compute P/E Ratio (Price / Earnings Per Share)
    pe_ratio = kplc_market_price / eps_value
    print(f"Calculated KPLC P/E Ratio: {pe_ratio:.2f}")
else:
    print("Could not isolate the EPS line structure automatically. Reviewing raw layout variations might be required.")
