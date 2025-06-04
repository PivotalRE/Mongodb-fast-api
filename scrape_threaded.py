import time
import random
import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

# ---------- Logging Setup ----------
logging.basicConfig(filename="scrape_errors.log", level=logging.ERROR, format='%(asctime)s - %(message)s')

# ---------- Fetch Function with Retry ----------
def fetch_grade(apn, max_retries=2):
    for attempt in range(max_retries):
        try:
            options = Options()
            options.add_argument("--headless=new")
            options.add_argument("--disable-gpu")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
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

            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            wait = WebDriverWait(driver, 10)

            url = f"https://blue.kingcounty.com/Assessor/eRealProperty/Dashboard.aspx?ParcelNbr={apn}"
            driver.get(url)

            time.sleep(random.uniform(0.3, 1.2))  # Optional polite delay

            grade_element = wait.until(EC.presence_of_element_located((By.XPATH,
                "/html/body/form/table/tbody/tr/td[2]/table/tbody/tr[2]/td[1]/table/tbody/tr[4]/td/table/tbody/tr/td[1]/div/table/tbody/tr[5]/td[2]"
            )))
            grade = grade_element.text.strip()
            driver.quit()
            return apn, grade

        except Exception as e:
            if 'driver' in locals():
                driver.quit()
            if attempt == max_retries - 1:
                logging.error(f"APN: {apn} | Failed after {max_retries} attempts | Error: {e}")
                return apn, "Error"
            time.sleep(1)  # Backoff before retry

# ---------- Load Input File ----------
df_apns = pd.read_csv("/home/elisha-a/pvl/all properties in reisift (1).csv", dtype={'Apn': str})
df_apns['Apn'] = df_apns['Apn'].str.zfill(10)
df_apns = df_apns.head(150000)
df_apns['Grade'] = None

# ---------- Start Timer ----------
start_time = time.time()

# ---------- Multithreaded Scraping ----------
with ThreadPoolExecutor(max_workers=6) as executor:
    futures = [executor.submit(fetch_grade, apn) for apn in df_apns['Apn']]
    for idx, future in enumerate(as_completed(futures), 1):
        apn, grade = future.result()
        df_apns.loc[df_apns['Apn'] == apn, 'Grade'] = grade
        print(f"APN: {apn}, Grade: {grade}")

        # Periodic autosave
        if idx % 1000 == 0:
            df_apns.to_csv("/home/elisha-a/pvl/output_temp.csv", index=False)
            print(f"💾 Autosaved at {idx} entries...")

# ---------- Final Save ----------
df_apns.to_csv("/home/elisha-a/pvl/output.csv", index=False)

# ---------- End Timer ----------
end_time = time.time()
elapsed = round((end_time - start_time) / 60, 2)
print(f"\n✅ Completed 150,000 records in ~{elapsed} minutes")
