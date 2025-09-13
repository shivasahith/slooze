#!/usr/bin/env python3
"""
Robust IndiaMART scraper (single master CSV output)

Usage examples:
  # interactive (asks for keyword & pages)
  python crawler.py

  # command-line
  python crawler.py --keyword furniture --pages 3

Outputs:
  - data/products.csv (master; appended; deduped by product_url)
  - data/page_{keyword}_{page}.html (saved raw HTML for debugging)
"""

import os
import re
import time
import argparse
import requests
import csv
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import pandas as pd

# ---- CONFIG ----
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
POLITE_DELAY = 1.8
DATA_DIR = "data"
MASTER_CSV = os.path.join(DATA_DIR, "products.csv")
os.makedirs(DATA_DIR, exist_ok=True)


def save_page_html(keyword, page_num, html):
    path = os.path.join(DATA_DIR, f"page_{keyword}_{page_num}.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print("Saved raw HTML ->", path)


def find_product_anchor_candidates(soup):
    """Return anchors that likely point to product detail pages."""
    anchors = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        # heuristics: proddetail, cardlinks class or long product URLs
        cls = " ".join(a.get("class", [])).lower() if a.get("class") else ""
        if ("proddetail" in href.lower()
                or "proddetail" in cls
                or "cardlinks" in cls
                or re.search(r"/proddetail/|/product-detail/|/proddetail", href, re.I)):
            anchors.append(a)
    return anchors


def get_ancestor_with_fields(a_tag, max_up=6):
    """Climb up to find a parent block that contains price/company/location info."""
    node = a_tag
    for _ in range(max_up):
        node = node.parent
        if node is None:
            break
        # heuristics for product block
        if node.select_one("p.price") or node.select_one("div.companyname") \
           or node.select_one("div.newLocationUi") or node.select_one("span.highlight") \
           or node.select_one("div.supplierInfoDiv") or node.select_one("p.prc") or node.select_one("div.card"):
            return node
    # fallback
    return a_tag.parent if a_tag.parent is not None else a_tag


def clean_text(el):
    if not el:
        return None
    return " ".join(el.get_text(" ", strip=True).split())


def absolute_url(base, href):
    return urljoin(base, href)


def load_existing_urls(csv_path):
    """Return a set of product URLs already present in the master CSV (if any)."""
    if not os.path.exists(csv_path):
        return set()
    try:
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        # find a URL-like column
        url_col = None
        for c in df.columns:
            if "url" in c.lower() or "link" in c.lower() or "href" in c.lower():
                url_col = c
                break
        if url_col:
            return set(df[url_col].dropna().astype(str).unique())
        # fallback: scan all columns for values that look like product urls
        urls = set()
        for c in df.columns:
            sample = df[c].dropna().astype(str)
            candidate = sample[sample.str.contains("proddetail|indiamart|http", case=False, na=False)]
            for v in candidate:
                urls.add(v)
        return urls
    except Exception as e:
        print("Warning: could not read existing CSV for dedupe:", e)
        return set()


def extract_from_block(a, block, base_url):
    # Title
    title = clean_text(a) or a.get("title") or a.get("aria-label") or None

    # URL
    href = a.get("href", "").strip()
    product_url = absolute_url(base_url, href) if href else None

    # Price
    price = None
    for sel in ["p.price", "p.prc", "span.p_price", "span.price", "div.price", "p.price_info"]:
        el = block.select_one(sel)
        if el:
            txt = el.get_text(" ", strip=True)
            if txt:
                price = txt
                break
    if not price:
        # fallback: find first ₹ or 'Ask' in block text
        t = block.get_text(" ", strip=True)
        m = re.search(r"(₹\s?[\d,]+(?:\.\d+)?(?:\s*/\s*\w+)?|Ask Price|Ask for Price|Ask)", t, re.I)
        if m:
            price = m.group(1).strip()
    if not price:
        price = None

    # Company / Seller
    company = None
    comp_el = block.select_one("div.companyname a.cardlinks") or block.select_one("div.companyname a") \
              or block.select_one("a.cardlinks.elps.elps1") or block.select_one("div.supplierInfoDiv a")
    if comp_el:
        company = clean_text(comp_el)
    else:
        c = block.select_one("div.companyname") or block.select_one("span.comp-name") or block.select_one("div.supplierInfoDiv")
        company = clean_text(c) if c else None

    # Location
    location = None
    loc_el = block.select_one("div.newLocationUi span.highlight") or block.select_one("span.highlight") \
             or block.select_one("div.supplierLocation, span.city")
    if loc_el:
        location = clean_text(loc_el)
    else:
        # fallback: try to find a capitalized token that looks like a city
        mloc = re.search(r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})?)\b", block.get_text(" ", strip=True))
        location = mloc.group(1) if mloc else None

    return {
        "scrape_time": datetime.utcnow().isoformat(),
        "keyword": None,  # will set later
        "product_name": title,
        "product_url": product_url,
        "seller": company,
        "location": location,
        "price_text": price
    }


def scrape_page(keyword, page_num, seen_urls):
    url = f"https://dir.indiamart.com/search.mp?ss={keyword}&pg={page_num}"
    print(f"\n🔎 Fetching page {page_num} -> {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
    except Exception as e:
        print("Request error:", e)
        return []

    if resp.status_code != 200:
        print("Non-200 status:", resp.status_code)
        save_page_html(keyword, page_num, resp.text)
        return []

    html = resp.text
    save_page_html(keyword, page_num, html)
    soup = BeautifulSoup(html, "html.parser")

    anchors = find_product_anchor_candidates(soup)
    print("Found product-like anchors:", len(anchors))

    # fallback: also include anchors with class 'cardlinks' if none found
    if not anchors:
        anchors = [a for a in soup.find_all("a", class_=lambda v: v and "cardlinks" in " ".join(v).lower())]
        print("Fallback anchors by class 'cardlinks' ->", len(anchors))

    rows = []
    for a in anchors:
        href = a.get("href", "").strip()
        if not href:
            continue
        full_url = absolute_url(resp.url, href)
        if full_url in seen_urls:
            # skip previously scraped product
            continue

        block = get_ancestor_with_fields(a, max_up=6)
        rec = extract_from_block(a, block, resp.url)
        rec["keyword"] = keyword
        # product_url may be None sometimes; skip those
        if not rec["product_url"]:
            continue
        rows.append(rec)
        seen_urls.add(rec["product_url"])

    print(f"→ Collected {len(rows)} unique new items from page {page_num}")
    return rows


def append_to_master(rows, master_csv=MASTER_CSV):
    if not rows:
        return 0
    file_exists = os.path.exists(master_csv)
    keys = list(rows[0].keys())
    # ensure write mode uses newline="" to avoid blank lines on Windows
    with open(master_csv, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def main(keyword=None, pages=3):
    if not keyword:
        keyword = input("Enter search keyword (e.g. electronics, shoes, furniture): ").strip()
    keyword = keyword.strip()
    if not keyword:
        print("No keyword provided, exiting.")
        return

    seen_urls = load_existing_urls(MASTER_CSV)
    print(f"Existing product URLs in master CSV: {len(seen_urls)}")

    all_new = []
    for p in range(1, pages + 1):
        new_rows = scrape_page(keyword, p, seen_urls)
        if new_rows:
            # append progressively to reduce memory and for immediate persistence
            appended = append_to_master(new_rows, MASTER_CSV)
            print(f"Appended {appended} rows to {MASTER_CSV}")
        else:
            print("No new rows from this page.")
        time.sleep(POLITE_DELAY)

    # summary
    try:
        df = pd.read_csv(MASTER_CSV)
        print("\n📊 Master CSV row count:", len(df))
    except Exception:
        print("\n📊 Master CSV updated (open file to check).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", "-k", help="Search keyword (e.g. electronics)", default=None)
    parser.add_argument("--pages", "-p", type=int, help="Number of pages to scrape", default=3)
    args = parser.parse_args()
    main(keyword=args.keyword, pages=args.pages)
