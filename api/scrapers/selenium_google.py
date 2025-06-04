import os
import time
import re
import random
import logging
import urllib.parse

from selenium import webdriver # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.support.ui import WebDriverWait # type: ignore
from selenium.webdriver.support import expected_conditions as EC # type: ignore
from selenium.common.exceptions import TimeoutException # type: ignore
from webdriver_manager.chrome import ChromeDriverManager # type: ignore
from selenium.webdriver.chrome.service import Service   # type: ignore

logger = logging.getLogger(__name__)

# Ensure WDM uses local cache to avoid repeat downloads
os.environ["WDM_LOCAL"] = "1"

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
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
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver

def handle_cookies(driver):
    try:
        consent_button = WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Accept')] | //div[text()='Accept all']"))
        )
        consent_button.click()
        time.sleep(1)
    except TimeoutException:
        pass

def get_parcel_number(search_term: str, candidate_id: str) -> str:
    driver = get_driver()
    try:
        query = urllib.parse.quote_plus(search_term)
        driver.get(f"https://www.google.com/search?q={query}")
        handle_cookies(driver)

        try:
            links = WebDriverWait(driver, 30).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a[href*='.gov']"))
            )
            for link in links[:3]:
                url = link.get_attribute("href")
                if "zillow.com" in url:
                    continue
                try:
                    driver.get(url)
                    WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    page_source = driver.page_source
                    patterns = [
                        r"Parcel Number[:\s]*(\d{10})",
                        r"Parcel ID[:\s]*(\d{10})",
                        r"Tax Parcel[:\s]*(\d{10})",
                        r"Parcel Number[:\s]*(\d{3}-\d{3}-\d{3})",
                        r"Parcel ID[:\s]*(\d{3}-\d{3}-\d{3})"
                    ]
                    for pattern in patterns:
                        match = re.search(pattern, page_source, re.IGNORECASE)
                        if match:
                            apn = match.group(1).replace("-", "")
                            logger.info(f"[Google] Found APN via {pattern} on {url}: {apn}")
                            return apn
                    driver.back()
                    time.sleep(random.uniform(1, 3))
                except Exception as e:
                    logger.warning(f"Failed to scrape {url}: {e}")
                    driver.back()
        except TimeoutException:
            logger.warning(f"No gov links found for {candidate_id}, checking snippets")

        # Fallback to search result snippets
        snippets = driver.find_elements(By.XPATH, "//div[contains(@class, 'VwiC3b')]")
        patterns = [
            r"Parcel Number[:\s]*(\d{10})",
            r"Parcel ID[:\s]*(\d{10})",
            r"Tax Parcel[:\s]*(\d{10})",
            r"Parcel Number[:\s]*(\d{3}-\d{3}-\d{3})",
            r"Parcel ID[:\s]*(\d{3}-\d{3}-\d{3})"
        ]
        for snippet in snippets:
            text = snippet.text.replace(",", "")
            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    apn = match.group(1).replace("-", "")
                    logger.info(f"[Google Snippet] Found APN: {apn}")
                    return apn

        logger.warning(f"No APN found for candidate {candidate_id}")
        with open(f"debug_google_{candidate_id}.html", "w") as f:
            f.write(driver.page_source)
        return None

    except Exception as e:
        logger.error(f"Parcel lookup failed for {candidate_id}: {e}")
        if "recaptcha" in driver.page_source.lower() or "captcha" in str(e).lower():
            logger.error(f"CAPTCHA detected during lookup for {candidate_id}")
        with open(f"debug_google_{candidate_id}_error.html", "w") as f:
            f.write(driver.page_source)
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    test_search = "123 Main St, Seattle, WA parcel number -site:zillow.com"
    apn = get_parcel_number(test_search, "test")
    print(f"APN for {test_search}: {apn}")
