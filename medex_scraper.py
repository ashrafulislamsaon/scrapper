"""
DeliMedi - Medex.com.bd Medicine Scraper (Cloudscraper Edition)
================================================================
Uses cloudscraper to bypass Cloudflare/bot protection on Medex.

Setup:
    pip install requests beautifulsoup4 pandas cloudscraper

Usage:
    python medex_scraper.py

Output:
    delimedi_medicines.csv
    delimedi_medicines.json
"""

import cloudscraper
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import random
import os
import re
import logging
import sys
from datetime import datetime

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
DELAY_MIN       = 3.0    # min seconds between requests (be gentle)
DELAY_MAX       = 6.0    # max seconds
RETRY_TIMES     = 3      # retries per URL
RETRY_WAIT      = 15.0   # wait before retry
SAVE_EVERY      = 50     # save every N records
OUTPUT_CSV      = "delimedi_medicines.csv"
OUTPUT_JSON     = "delimedi_medicines.json"
FAILED_URLS_LOG = "failed_urls.txt"
PROGRESS_FILE   = "scrape_progress.json"
URL_CACHE_FILE  = "medicine_urls.json"

BASE_URL  = "https://medex.com.bd"
ALPHABETS = list("abcdefghijklmnopqrstuvwxyz")


# ─────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────
def setup_logging():
    logger = logging.getLogger("delimedi")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler("scraper.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    try:
        ch = logging.StreamHandler(
            open(sys.stdout.fileno(), mode='w', encoding='utf-8',
                 closefd=False, buffering=1)
        )
    except Exception:
        ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()


# ─────────────────────────────────────────────
# CLOUDSCRAPER SESSION
# ─────────────────────────────────────────────
def make_scraper():
    """
    Creates a cloudscraper instance that mimics a real Chrome browser
    and can bypass Cloudflare JS challenges automatically.
    """
    scraper = cloudscraper.create_scraper(
        browser={
            "browser": "chrome",
            "platform": "windows",
            "mobile": False
        }
    )
    # Visit homepage first to establish session/cookies
    try:
        logger.info("Warming up session - visiting Medex homepage...")
        scraper.get(BASE_URL, timeout=20)
        logger.info("Session ready.")
        time.sleep(random.uniform(3, 5))
    except Exception as e:
        logger.warning(f"Homepage visit failed: {e}")
    return scraper


def random_delay():
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ─────────────────────────────────────────────
# FETCH WITH RETRY
# ─────────────────────────────────────────────
def fetch(scraper, url):
    for attempt in range(1, RETRY_TIMES + 1):
        try:
            resp = scraper.get(url, timeout=20)

            if resp.status_code == 429:
                wait = RETRY_WAIT * attempt * 2
                logger.warning(f"Rate limited. Waiting {wait}s...")
                time.sleep(wait)
                continue

            if resp.status_code == 403:
                logger.warning(f"Blocked (403) attempt {attempt}. Waiting {RETRY_WAIT * attempt}s...")
                time.sleep(RETRY_WAIT * attempt)
                continue

            if resp.status_code == 200:
                return resp

        except Exception as e:
            logger.warning(f"Attempt {attempt} failed for {url}: {e}")
            time.sleep(RETRY_WAIT)

    logger.error(f"All {RETRY_TIMES} attempts failed for: {url}")
    return None


# ─────────────────────────────────────────────
# STEP 1: COLLECT MEDICINE URLs
# ─────────────────────────────────────────────
def extract_medicine_links(soup):
    found = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if re.search(r"/brands/\d+/", href):
            clean = href.split("?")[0]
            if not clean.startswith("http"):
                clean = BASE_URL + clean
            found.add(clean)
    return found


def get_last_page(soup):
    max_page = 1
    for a in soup.find_all("a", href=True):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            p = int(m.group(1))
            if p > max_page:
                max_page = p
    return max_page


def get_all_medicine_urls(scraper):
    all_urls = set()

    for idx, letter in enumerate(ALPHABETS, 1):
        logger.info(f"[{idx}/{len(ALPHABETS)}] Letter: {letter.upper()}")

        first_url = f"{BASE_URL}/brands?alpha={letter}&page=1"
        resp = fetch(scraper, first_url)
        if not resp:
            logger.error(f"Skipping letter {letter}")
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        links = extract_medicine_links(soup)
        all_urls.update(links)
        last_page = get_last_page(soup)
        logger.info(f"  {letter.upper()}: {last_page} pages | +{len(links)} | Total: {len(all_urls)}")

        for page in range(2, last_page + 1):
            page_url = f"{BASE_URL}/brands?alpha={letter}&page={page}"
            resp2 = fetch(scraper, page_url)
            if resp2:
                soup2 = BeautifulSoup(resp2.text, "html.parser")
                new_links = extract_medicine_links(soup2)
                all_urls.update(new_links)
                logger.info(f"  {letter.upper()} p{page}/{last_page} | +{len(new_links)} | Total: {len(all_urls)}")
            random_delay()

        random_delay()

    return list(all_urls)


# ─────────────────────────────────────────────
# STEP 2: SCRAPE DETAIL PAGE
# ─────────────────────────────────────────────
def is_empty(med):
    return all(med.get(f, "") == "" for f in ["brand_name", "generic_name", "manufacturer"])


def scrape_medicine_detail(scraper, url):
    resp = fetch(scraper, url)
    if not resp:
        return None

    try:
        soup = BeautifulSoup(resp.text, "html.parser")

        med = {
            "url":               url,
            "medicine_id":       "",
            "brand_name":        "",
            "dosage_form":       "",
            "generic_name":      "",
            "strength":          "",
            "manufacturer":      "",
            "unit_price":        "",
            "strip_price":       "",
            "pack_size":         "",
            "image_url":         "",
            "indications":       "",
            "composition":       "",
            "description":       "",
            "pharmacology":      "",
            "dosage_admin":      "",
            "contraindications": "",
            "side_effects":      "",
            "pregnancy":         "",
            "therapeutic_class": "",
            "storage":           "",
            "scraped_at":        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        # Medicine ID
        id_match = re.search(r"/brands/(\d+)/", url)
        if id_match:
            med["medicine_id"] = id_match.group(1)

        # Brand + Dosage form
        h1 = soup.select_one("h1.brand")
        if h1:
            small = h1.find("small")
            if small:
                med["dosage_form"] = small.get_text(strip=True)
                small.decompose()
            med["brand_name"] = h1.get_text(strip=True)

        # Generic name
        generic_div = soup.find("div", title="Generic Name")
        if generic_div:
            a = generic_div.find("a")
            med["generic_name"] = a.get_text(strip=True) if a else generic_div.get_text(strip=True)

        # Strength
        strength_div = soup.find("div", title="Strength")
        if strength_div:
            med["strength"] = strength_div.get_text(strip=True)

        # Manufacturer
        mfr_div = soup.find("div", title="Manufactured by")
        if mfr_div:
            a = mfr_div.find("a")
            med["manufacturer"] = a.get_text(strip=True) if a else mfr_div.get_text(strip=True)

        # Price
        pkg = soup.select_one("div.package-container")
        if pkg:
            pkg_text = pkg.get_text(separator="\n")
            unit_m  = re.search(r"Unit\s*Price[^\d]*([\d,\.]+)", pkg_text)
            strip_m = re.search(r"Strip\s*Price[^\d]*([\d,\.]+)", pkg_text)
            pack_m  = re.search(r"\((\d+\s*[xX]\s*\d+)\s*[:\s]", pkg_text)
            if unit_m:  med["unit_price"]  = unit_m.group(1).replace(",", "")
            if strip_m: med["strip_price"] = strip_m.group(1).replace(",", "")
            if pack_m:  med["pack_size"]   = pack_m.group(1).strip()

        # Image
        img_tag = soup.select_one("img.img-defer[data-src]")
        if img_tag:
            src = img_tag.get("data-src", "")
            if src:
                med["image_url"] = src if src.startswith("http") else BASE_URL + src
        if not med["image_url"]:
            btn = soup.select_one("a.mp-trigger[href*='/storage/']")
            if btn:
                href = btn.get("href", "")
                med["image_url"] = href if href.startswith("http") else BASE_URL + href

        # Content sections
        section_map = {
            "indications":        "indications",
            "composition":        "composition",
            "description":        "description",
            "mode_of_action":     "pharmacology",
            "dosage":             "dosage_admin",
            "contraindications":  "contraindications",
            "side_effects":       "side_effects",
            "pregnancy_cat":      "pregnancy",
            "drug_classes":       "therapeutic_class",
            "storage_conditions": "storage",
        }
        for sid, fname in section_map.items():
            div = soup.find("div", id=sid)
            if div:
                parent = div.find_parent("div") or div
                body = parent.find_next_sibling("div", class_="ac-body")
                if body:
                    med[fname] = body.get_text(separator=" ", strip=True)

        if is_empty(med):
            logger.warning(f"Still blocked for: {url}")
            return None

        return med

    except Exception as e:
        logger.error(f"Parse error {url}: {e}")
        return None


# ─────────────────────────────────────────────
# SAVE / LOAD
# ─────────────────────────────────────────────
def save_data(medicines):
    df = pd.DataFrame(medicines)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(medicines, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(medicines)} records to {OUTPUT_CSV}")


def save_progress(scraped_urls):
    with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
        json.dump({"scraped": list(scraped_urls)}, f)


def load_good_medicines():
    """Load previously saved medicines that actually have data."""
    if not os.path.exists(OUTPUT_JSON):
        return [], set()
    try:
        with open(OUTPUT_JSON, encoding="utf-8") as f:
            all_saved = json.load(f)
        good = [m for m in all_saved if m.get("brand_name", "")]
        good_urls = {m["url"] for m in good}
        logger.info(f"Loaded {len(good)} valid records from previous run")
        return good, good_urls
    except Exception:
        return [], set()


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    logger.info("=" * 60)
    logger.info("DeliMedi Medex Scraper - Cloudscraper Edition")
    logger.info("=" * 60)

    scraper = make_scraper()

    # ── Phase 1: Get URLs ────────────────────────────────────────
    if os.path.exists(URL_CACHE_FILE):
        with open(URL_CACHE_FILE, encoding="utf-8") as f:
            all_urls = json.load(f)
        logger.info(f"Loaded {len(all_urls)} URLs from cache")
    else:
        logger.info("Phase 1: Collecting medicine URLs...")
        all_urls = get_all_medicine_urls(scraper)
        with open(URL_CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(all_urls, f)
        logger.info(f"Phase 1 done. {len(all_urls)} URLs saved.")

    if not all_urls:
        logger.error("No URLs found. Check your connection.")
        return

    # ── Phase 2: Scrape details ──────────────────────────────────
    logger.info("Phase 2: Scraping detail pages...")

    medicines, good_urls = load_good_medicines()
    remaining = [u for u in all_urls if u not in good_urls]
    logger.info(f"To scrape: {len(remaining)} | Already done: {len(good_urls)}")

    failed_urls  = []
    scraped_urls = set(good_urls)

    for i, url in enumerate(remaining, 1):
        # Refresh scraper every 300 requests
        if i % 300 == 0:
            logger.info("Refreshing cloudscraper session...")
            scraper = make_scraper()

        data = scrape_medicine_detail(scraper, url)

        if data:
            medicines.append(data)
            scraped_urls.add(url)
        else:
            failed_urls.append(url)
            with open(FAILED_URLS_LOG, "a", encoding="utf-8") as f:
                f.write(url + "\n")

        if i % 10 == 0:
            logger.info(f"Progress: {i}/{len(remaining)} | OK: {len(medicines)} | Failed: {len(failed_urls)}")

        if i % SAVE_EVERY == 0:
            save_data(medicines)
            save_progress(scraped_urls)

        random_delay()

    save_data(medicines)
    save_progress(scraped_urls)

    logger.info("=" * 60)
    logger.info(f"DONE. Total: {len(medicines)} | Failed: {len(failed_urls)}")
    logger.info(f"Output: {OUTPUT_CSV} | {OUTPUT_JSON}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()