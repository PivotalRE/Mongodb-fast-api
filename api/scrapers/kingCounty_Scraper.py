import time
import random
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from pymongo import MongoClient
from datetime import datetime


def scrape_king_county_properties(
    input_csv: str,
    output_csv: str,
    max_rows: int = None
):
    # ---------- Logging ----------
    logging.basicConfig(
        filename="scrape_errors.log",
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # ---------- Load CSV ----------
    df = pd.read_csv(input_csv, dtype=str)
    if 'Parcel id' not in df.columns:
        raise ValueError("Column 'Parcel id' not found in the CSV.")
    df['Parcel id'] = df['Parcel id'].str.zfill(10)

    # ---------- Columns to Scrape ----------
    scraped_columns = [
        'Grade', 'Sale Price', 'Sale Instrument', 'Sale Reason', 'Nuisance',
        'Views', 'Waterfront', 'Condition', 'Zoning', 'Sewer/Septic',
        'Appraised Imps Value', 'Appraised Total Value', 'Year Built', 'Document Date'
    ]
    scraped_data = pd.DataFrame(columns=['Parcel id'] + scraped_columns)

    # ---------- Setup Chrome ----------
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--remote-debugging-port=9222")
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    wait = WebDriverWait(driver, 10)

    def extract_table_data(table_element):
        data = {}
        rows = table_element.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) == 2:
                data[cells[0].text.strip()] = cells[1].text.strip()
        return data

    # ---------- Scrape ----------
    total_start = time.time()
    for index, row in df.iterrows():
        if max_rows and len(scraped_data) >= max_rows:
            break

        apn = row['Parcel id']

        # ✅ Skip invalid APNs (not 10 digits or non-numeric)
        if len(apn) != 10 or not apn.isdigit():
            logging.warning(f"[⏭️] Skipping invalid APN: {apn}")
            print(f"[⏭️] Skipping invalid APN: {apn}")
            continue

        result = {'Parcel id': apn}
        start_time = time.time()

        try:
            url = f"https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr={apn}"
            driver.get(url)

            grade_xpath = "/html/body/form/table/tbody/tr/td[2]/table/tbody/tr[2]/td[1]/table/tbody/tr[4]/td/table/tbody/tr/td[1]/div/table/tbody/tr[5]/td[2]"
            grade_element = wait.until(EC.presence_of_element_located((By.XPATH, grade_xpath)))
            result['Grade'] = grade_element.text

            wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Property Detail"))).click()
            wait.until(EC.url_contains("Detail.aspx"))
            time.sleep(1)

            try:
                sale_table = wait.until(EC.presence_of_element_located((By.ID, "cphContent_GridViewSales")))
                rows = sale_table.find_elements(By.TAG_NAME, "tr")
                if len(rows) > 1:
                    cells = rows[1].find_elements(By.TAG_NAME, "td")
                    result['Sale Price'] = cells[3].text.strip()
                    result['Sale Instrument'] = cells[6].text.strip()
                    result['Sale Reason'] = cells[7].text.strip()
                    result['Document Date'] = cells[2].text.strip()
            except: pass

            try:
                tax_table = driver.find_element(By.ID, "cphContent_GridViewTaxRoll")
                rows = tax_table.find_elements(By.TAG_NAME, "tr")
                if len(rows) > 1:
                    cells = rows[1].find_elements(By.TAG_NAME, "td")
                    result['Appraised Imps Value'] = cells[6].text.strip()
                    result['Appraised Total Value'] = cells[7].text.strip()
            except: pass

            for field, col in [
                ('cphContent_DetailsViewLandNuisances', 'Nuisance'),
                ('cphContent_DetailsViewLandViews', 'Views'),
                ('cphContent_DetailsViewLandWaterfront', 'Waterfront')
            ]:
                try:
                    result[col] = str(extract_table_data(driver.find_element(By.ID, field)))
                except:
                    result[col] = "Not Found"

            try:
                cond_xpath = "//*[@id='cphContent_DetailsViewResBldg']/tbody/tr[8]/td[2]"
                result['Condition'] = driver.find_element(By.XPATH, cond_xpath).text.strip()
            except:
                result['Condition'] = "Not Found"

            try:
                for row_ in driver.find_element(By.ID, 'cphContent_DetailsViewResBldg').find_elements(By.TAG_NAME, "tr"):
                    cells = row_.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2 and "Year Built" in cells[0].text:
                        result['Year Built'] = cells[1].text.strip()
            except: pass

            try:
                land_rows = driver.find_element(By.ID, 'cphContent_DetailsViewLandSystem').find_elements(By.TAG_NAME, "tr")
                for row_ in land_rows:
                    cells = row_.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2:
                        if "Zoning" in cells[0].text:
                            result['Zoning'] = cells[1].text.strip()
                        if "Sewer/Septic" in cells[0].text:
                            result['Sewer/Septic'] = cells[1].text.strip()
            except: pass

            print(f"\n[📄 Row {index + 1}] APN {apn} scraped:")
            print(result)
            scraped_data = pd.concat([scraped_data, pd.DataFrame([result])], ignore_index=True)
            logging.info(f"[✔️] APN {apn} scraped in {round(time.time() - start_time, 2)}s")

        except Exception as e:
            logging.error(f"[✘] APN {apn} failed: {str(e)}")

    driver.quit()

    # ---------- Merge with Original Data by Parcel id ----------
    merged = pd.merge(df, scraped_data, on='Parcel id', how='left', suffixes=('', '_scraped'))

    # Fill only scraped columns back in place
    for col in scraped_columns:
        if col in merged:
            df[col] = merged[col]

    df.to_csv(output_csv, index=False)
    logging.info(f"✅ Done. Scraped data saved to {output_csv} in {round(time.time() - total_start, 2)}s")
    print(f"\n✅ Scraped data successfully saved to {output_csv}")

def scrape_from_mongo_and_update(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    limit: int = None
):
    # Setup MongoDB connection
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    apn_query = collection.find({}, {"apn": 1}).limit(limit or 0)
    apns = [doc["apn"] for doc in apn_query if "apn" in doc and str(doc["apn"]).isdigit()]

    # Setup WebDriver (reuse existing logic)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"
    ]
    options.add_argument(f"user-agent={random.choice(user_agents)}")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    wait = WebDriverWait(driver, 10)

    # Scraping logic reused
    def extract_table_data(table_element):
        data = {}
        rows = table_element.find_elements(By.TAG_NAME, "tr")
        for row in rows:
            cells = row.find_elements(By.TAG_NAME, "td")
            if len(cells) == 2:
                data[cells[0].text.strip()] = cells[1].text.strip()
        return data

    for i, apn in enumerate(apns, start=1):
        try:
            if len(apn) != 10:
                continue

            result = {}
            start_time = time.time()
            url = f"https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr={apn}"
            driver.get(url)

            grade_xpath = "/html/body/form/table/tbody/tr/td[2]/table/tbody/tr[2]/td[1]/table/tbody/tr[4]/td/table/tbody/tr/td[1]/div/table/tbody/tr[5]/td[2]"
            grade_element = wait.until(EC.presence_of_element_located((By.XPATH, grade_xpath)))
            result['Grade'] = grade_element.text

            wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Property Detail"))).click()
            wait.until(EC.url_contains("Detail.aspx"))
            time.sleep(1)

            try:
                sale_table = wait.until(EC.presence_of_element_located((By.ID, "cphContent_GridViewSales")))
                rows = sale_table.find_elements(By.TAG_NAME, "tr")
                if len(rows) > 1:
                    cells = rows[1].find_elements(By.TAG_NAME, "td")
                    result['Sale Price'] = cells[3].text.strip()
                    result['Sale Instrument'] = cells[6].text.strip()
                    result['Sale Reason'] = cells[7].text.strip()
                    result['Document Date'] = cells[2].text.strip()
            except: pass

            try:
                tax_table = driver.find_element(By.ID, "cphContent_GridViewTaxRoll")
                rows = tax_table.find_elements(By.TAG_NAME, "tr")
                if len(rows) > 1:
                    cells = rows[1].find_elements(By.TAG_NAME, "td")
                    result['Appraised Imps Value'] = cells[6].text.strip()
                    result['Appraised Total Value'] = cells[7].text.strip()
            except: pass

            for field, col in [
                ('cphContent_DetailsViewLandNuisances', 'Nuisance'),
                ('cphContent_DetailsViewLandViews', 'Views'),
                ('cphContent_DetailsViewLandWaterfront', 'Waterfront')
            ]:
                try:
                    result[col] = str(extract_table_data(driver.find_element(By.ID, field)))
                except:
                    result[col] = "Not Found"

            try:
                cond_xpath = "//*[@id='cphContent_DetailsViewResBldg']/tbody/tr[8]/td[2]"
                result['Condition'] = driver.find_element(By.XPATH, cond_xpath).text.strip()
            except:
                result['Condition'] = "Not Found"

            try:
                for row_ in driver.find_element(By.ID, 'cphContent_DetailsViewResBldg').find_elements(By.TAG_NAME, "tr"):
                    cells = row_.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2 and "Year Built" in cells[0].text:
                        result['Year Built'] = cells[1].text.strip()
            except: pass

            try:
                land_rows = driver.find_element(By.ID, 'cphContent_DetailsViewLandSystem').find_elements(By.TAG_NAME, "tr")
                for row_ in land_rows:
                    cells = row_.find_elements(By.TAG_NAME, "td")
                    if len(cells) == 2:
                        if "Zoning" in cells[0].text:
                            result['Zoning'] = cells[1].text.strip()
                        if "Sewer/Septic" in cells[0].text:
                            result['Sewer/Septic'] = cells[1].text.strip()
            except: pass

            collection.update_one(
                {"apn": apn},
                {"$set": {
                    "scraped_data": result,
                    "scraped_at": datetime.utcnow()
                }}
            )

            logging.info(f"[✔️] {i}/{len(apns)} - Scraped {apn} in {round(time.time() - start_time, 2)}s")

        except Exception as e:
            logging.error(f"[✘] {apn} failed: {str(e)}")

    driver.quit()
