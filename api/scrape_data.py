import time
import random
import pandas as pd
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException,
    StaleElementReferenceException, WebDriverException
)
from webdriver_manager.chrome import ChromeDriverManager

driver_path = ChromeDriverManager("134.0.0.0").install()

# -------------- Logging Setup --------------
logging.basicConfig(
    filename="social_scraper.log",
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filemode='w'
)
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())

# -------------- Compatible ChromeDriver Setup --------------
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-popup-blocking")
    options.add_argument("--disable-notifications")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])

    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    ]
    user_agent = random.choice(user_agents)
    options.add_argument(f"user-agent={user_agent}")

    # ✅ Automatically matches installed Chrome version
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

# -------------- Block Detection --------------
def is_blocked(driver):
    blocked_indicators = [
        "sorry", "captcha", "security check", "automated queries",
        "unusual traffic", "not a robot", "denied", "detected unusual"
    ]
    return any(indicator in driver.page_source.lower() for indicator in blocked_indicators)

# -------------- Cookie Consent --------------
def handle_cookies(driver):
    try:
        cookie_buttons = WebDriverWait(driver, 3).until(
            EC.presence_of_all_elements_located((
                By.XPATH, "//button[contains(., 'ccept') or contains(., 'gree')]"
            ))
        )
        random.choice(cookie_buttons).click()
        logger.debug("Accepted cookies")
        time.sleep(0.5)
    except (TimeoutException, NoSuchElementException):
        pass

# -------------- Google Search Execution --------------
def execute_search(driver, query, max_retries=3):
    for attempt in range(max_retries):
        try:
            driver.get("https://www.google.com")
            handle_cookies(driver)

            search_box = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.NAME, "q"))
            )
            search_box.clear()
            for char in query:
                search_box.send_keys(char)
                time.sleep(random.uniform(0.05, 0.15))
            search_box.send_keys(Keys.RETURN)

            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.ID, "search"))
            )

            if is_blocked(driver):
                raise RuntimeError("Google block detected")

            return True
        except Exception as e:
            logger.warning(f"Search attempt {attempt + 1} failed: {str(e)}")
            time.sleep(2 ** attempt + random.random())
    return False

# -------------- Extract Links --------------
def extract_links(driver):
    results = []
    selectors = ['div.g a', 'div.yuRUbf a', 'div.tF2Cxc a']
    for selector in selectors:
        try:
            links = driver.find_elements(By.CSS_SELECTOR, selector)
            for link in links:
                href = link.get_attribute("href")
                if href and href not in results:
                    results.append(href)
            if results:
                break
        except Exception:
            continue
    return results

# -------------- Social Link Search --------------
def search_social_links(driver, name):
    NOT_FOUND = "Not Found"
    query = f"{name} site:tiktok.com OR site:instagram.com"

    if not execute_search(driver, query):
        logger.error(f"Search failed for: {name}")
        return NOT_FOUND, NOT_FOUND

    links = extract_links(driver)
    logger.debug(f"Found {len(links)} links for {name}")

    tiktok = next((url for url in links if "tiktok.com" in url and '/@' in url), NOT_FOUND)
    instagram = next((url for url in links if "instagram.com" in url and '/p/' not in url), NOT_FOUND)

    return tiktok, instagram

# -------------- Main Scraping Function --------------
def main():
    try:
        input_path = "/home/elisha-a/pvl/api/sample (1).xlsx"
        df = pd.read_excel(input_path, dtype=str)
        df["TikTok"] = "Not Scraped"
        df["Instagram"] = "Not Scraped"
        logger.info(f"Loaded {len(df)} records from {input_path}")
    except Exception as e:
        logger.critical(f"Data loading failed: {str(e)}")
        return

    try:
        driver = get_driver()
        logger.info("Driver initialized successfully")
    except Exception as e:
        logger.critical(f"Driver initialization failed: {str(e)}")
        return

    total = len(df)
    for index, row in df.iterrows():
        full_name = f"{row['First Name']} {row['Last Name']}"
        logger.info(f"Processing ({index + 1}/{total}): {full_name}")

        try:
            tiktok, instagram = search_social_links(driver, full_name)
            df.at[index, "TikTok"] = tiktok
            df.at[index, "Instagram"] = instagram
            logger.info(f"Results: TikTok={tiktok}, Instagram={instagram}")
        except Exception as e:
            logger.error(f"Processing failed for {full_name}: {str(e)}")
            df.at[index, "TikTok"] = "Error"
            df.at[index, "Instagram"] = "Error"

        time.sleep(min(random.uniform(3.0, 8.0) + index * 0.1, 15))

    try:
        output_path = "/home/elisha-a/pvl/api/social_profiles_output.xlsx"
        df.to_excel(output_path, index=False)
        logger.info(f"Results saved to {output_path}")
    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")
    finally:
        driver.quit()
        logger.info("Driver closed")

if __name__ == "__main__":
    main()
