# coursera_full_courses_scraper.py
# -*- coding: utf-8 -*-

import time
import re
import random
import json
from typing import List, Dict, Tuple, Optional, Set
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, NoSuchElementException, JavascriptException

# =========================
# CONFIGURATION
# =========================
HEADLESS = True
TIMEOUT = 25                     # explicit wait timeout
SCROLL_PAUSE = (1.2, 2.0)        # pause between scroll batches
DETAIL_PAUSE = (1.2, 2.0)        # pause before parsing detail page
MAX_IDLE_SCROLLS = 6             # stop when no new results appear after N scroll rounds
RETRY_DETAIL = 2                 # retries per course page on transient failures
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0 Safari/537.36"
)

LANGUAGES = ["Java", "Python", "C", "C++", "JavaScript", "HTML", "CSS", "SQL"]

# Only show COURSES (not specializations/certs) to stay precise
SEARCH_URL = "https://www.coursera.org/search?query={q}&productTypeDescription=courses"

# =========================
# HELPERS
# =========================
def jitter(a: float, b: float):
    time.sleep(random.uniform(a, b))

def clean_text(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def normalize_level(text: str) -> Optional[str]:
    t = text.lower()
    if "beginner" in t: return "Beginner"
    if "intermediate" in t: return "Intermediate"
    if "advanced" in t: return "Advanced"
    return None

def extract_duration_like(text: str) -> Optional[str]:
    """Return a plausible duration string if it contains time signals."""
    t = text.lower()
    # More comprehensive time-related keywords
    time_keywords = [
        "hour", "hours", "hr", "hrs", 
        "week", "weeks", "wk", "wks",
        "month", "months", "mo", "mos",
        "day", "days", "minute", "minutes", "min", "mins",
        "approx", "approximately", "estimated", "complete",
        "pace", "commitment", "time"
    ]
    
    if any(k in t for k in time_keywords):
        # Clean and return the text
        cleaned = clean_text(text)
        # Remove common prefixes that aren't part of duration
        cleaned = re.sub(r'^(duration|time|estimated|approximately|approx):\s*', '', cleaned, flags=re.I)
        return cleaned
    return None

def parse_duration_from_text(text: str) -> Optional[str]:
    """Extract duration using regex patterns for common formats."""
    if not text:
        return None
    
    # Common duration patterns
    patterns = [
        r'(\d+(?:\.\d+)?)\s*(hour|hours|hr|hrs)\s*(?:per\s*week|weekly)?',
        r'(\d+(?:\.\d+)?)\s*(week|weeks|wk|wks)',
        r'(\d+(?:\.\d+)?)\s*(month|months|mo|mos)',
        r'(\d+(?:\.\d+)?)\s*(day|days)',
        r'(\d+(?:\.\d+)?)\s*(minute|minutes|min|mins)',
        r'approximately\s*(\d+(?:\.\d+)?)\s*(hour|hours|week|weeks)',
        r'about\s*(\d+(?:\.\d+)?)\s*(hour|hours|week|weeks)',
        r'(\d+(?:\.\d+)?)-(\d+(?:\.\d+)?)\s*(hour|hours|week|weeks)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text.lower())
        if match:
            # Return the matched portion with proper formatting
            return match.group(0).strip()
    
    return None

def build_driver() -> webdriver.Chrome:
    opts = Options()
    if HEADLESS:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1440,900")
    opts.add_argument(f"user-agent={USER_AGENT}")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.set_page_load_timeout(60)
    return driver

def wait_body(driver):
    WebDriverWait(driver, TIMEOUT).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

# =========================
# SEARCH PAGE HANDLING
# =========================
def collect_all_course_cards(driver) -> List[Tuple[str, str]]:
    """
    Infinite-scroll the search page and collect ALL course result links (title, href).
    Coursera loads more cards as you scroll; we stop after several idle rounds.
    """
    def get_links_from_dom() -> List[Tuple[str, str]]:
        soup = BeautifulSoup(driver.page_source, "lxml")
        links = []
        # Courses typically under /learn/
        for a in soup.select('a[href^="/learn/"]'):
            href = a.get("href")
            title = a.get("aria-label") or a.get_text(strip=True)
            if href and title:
                links.append((clean_text(title), "https://www.coursera.org" + href))
        # Dedup preserving order
        seen = set()
        uniq = []
        for t, u in links:
            if u not in seen:
                seen.add(u)
                uniq.append((t, u))
        return uniq

    # Initial snapshot
    results = get_links_from_dom()
    idle = 0

    while True:
        prev_count = len(results)
        # Try scrolling several times per round (helps trigger lazy loading)
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except JavascriptException:
            pass
        jitter(*SCROLL_PAUSE)

        # Sometimes results load when we scroll up a bit then down again
        try:
            driver.execute_script("window.scrollBy(0, -200);")
            driver.execute_script("window.scrollBy(0, 200);")
        except JavascriptException:
            pass
        jitter(*SCROLL_PAUSE)

        # Re-parse
        results = get_links_from_dom()
        if len(results) <= prev_count:
            idle += 1
        else:
            idle = 0

        if idle >= MAX_IDLE_SCROLLS:
            break

    return results

# =========================
# DETAIL PAGE PARSING
# =========================
def parse_course_detail(driver) -> Tuple[Optional[str], Optional[str], List[str]]:
    """
    Extract (duration, level, concepts[]) from the currently opened course page.
    We use multiple heuristics to be resilient to template changes.
    """
    soup = BeautifulSoup(driver.page_source, "lxml")

    # --- Duration ---
    duration = None
    
    # Strategy 1: Look for specific Coursera duration patterns
    duration_patterns = [
        # Common Coursera duration selectors
        '[data-testid*="duration"]',
        '[class*="duration"]',
        '[class*="time"]',
        '[class*="Duration"]',
        '[aria-label*="duration"]',
        '[aria-label*="time"]',
        '.rc-Duration',
        '.duration-text',
        '.course-duration'
    ]
    
    for pattern in duration_patterns:
        elements = soup.select(pattern)
        for el in elements:
            txt = clean_text(el.get_text(" ", strip=True))
            d = extract_duration_like(txt)
            if d and len(d) < 50:  # reasonable length check
                duration = d
                break
        if duration:
            break
    
    # Strategy 2: Look in structured data (JSON-LD or meta tags)
    if not duration:
        # Check for structured data
        scripts = soup.find_all('script', type='application/ld+json')
        for script in scripts:
            try:
                data = json.loads(script.string)
                if isinstance(data, dict):
                    # Look for duration in course data
                    if 'timeRequired' in data:
                        duration = data['timeRequired']
                        break
                    elif 'duration' in data:
                        duration = str(data['duration'])
                        break
            except:
                continue
    
    # Strategy 3: Look for common duration text patterns in specific areas
    if not duration:
        # Look in course info sections
        info_sections = soup.find_all(['div', 'section'], class_=lambda x: x and any(
            keyword in x.lower() for keyword in ['info', 'detail', 'overview', 'about', 'course-info']
        ))
        
        for section in info_sections:
            for el in section.find_all(["span", "div", "li", "p"]):
                txt = clean_text(el.get_text(" ", strip=True))
                # Look for patterns like "4 weeks", "20 hours", etc.
                if re.search(r'\b\d+\s*(hour|hours|week|weeks|month|months|day|days|hr|hrs|wk|wks)\b', txt, re.I):
                    d = extract_duration_like(txt)
                    if d and len(d) < 50:
                        duration = d
                        break
            if duration:
                break
    
    # Strategy 4: Look for duration in course metadata or sidebar
    if not duration:
        # Look for metadata sections, sidebars, or info boxes
        metadata_selectors = [
            '[class*="sidebar"]', '[class*="meta"]', '[class*="info"]',
            '[class*="details"]', '[class*="overview"]', '[class*="stats"]',
            '[class*="course-info"]', '[class*="CourseInfo"]'
        ]
        
        for selector in metadata_selectors:
            elements = soup.select(selector)
            for el in elements:
                txt = clean_text(el.get_text(" ", strip=True))
                d = parse_duration_from_text(txt)
                if d:
                    duration = d
                    break
            if duration:
                break
    
    # Strategy 5: Look for "Commitment" or "Time to complete" sections
    if not duration:
        commitment_keywords = ["commitment", "time to complete", "estimated time", "duration", "length"]
        for keyword in commitment_keywords:
            # Find elements that contain these keywords
            for el in soup.find_all(text=re.compile(keyword, re.I)):
                parent = el.parent
                if parent:
                    # Look in the same element or nearby elements
                    for sibling in [parent] + list(parent.find_next_siblings())[:3]:
                        txt = clean_text(sibling.get_text(" ", strip=True))
                        d = parse_duration_from_text(txt)
                        if d:
                            duration = d
                            break
                if duration:
                    break
            if duration:
                break
    
    # Strategy 6: Look in page title and headings
    if not duration:
        title_elements = soup.find_all(['title', 'h1', 'h2', 'h3'])
        for el in title_elements:
            txt = clean_text(el.get_text(" ", strip=True))
            d = parse_duration_from_text(txt)
            if d:
                duration = d
                break
    
    # Strategy 7: Look for common Coursera specific patterns in any text
    if not duration:
        # Look for text containing patterns like "X hours per week" or "Complete in X weeks"
        page_text = clean_text(soup.get_text(" "))
        duration_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:to\s*)?(\d+(?:\.\d+)?)?\s*(hours?|hrs?)\s*(?:per\s*week|weekly)', page_text, re.I)
        if not duration_match:
            duration_match = re.search(r'complete\s*in\s*(\d+(?:\.\d+)?)\s*(weeks?|months?|days?)', page_text, re.I)
        if not duration_match:
            duration_match = re.search(r'(\d+(?:\.\d+)?)\s*(weeks?|months?)\s*(?:to\s*complete|duration|long)', page_text, re.I)
        
        if duration_match:
            duration = duration_match.group(0).strip()
    
    # Strategy 8: Fallback - scan all elements but with better filtering
    if not duration:
        for el in soup.find_all(["span", "div", "li", "p"]):
            txt = clean_text(el.get_text(" ", strip=True))
            # Only consider elements with reasonable text length and duration patterns
            if 5 < len(txt) < 100:
                d = parse_duration_from_text(txt)
                if d:
                    duration = d
                    break

    # --- Level ---
    level = None
    for el in soup.find_all(["span", "div", "li"]):
        lev = normalize_level(el.get_text())
        if lev:
            level = lev
            break

    # --- Concepts / Skills ---
    concepts: List[str] = []

    # Strategy A: sections headed by "What you’ll learn" or "Skills you’ll gain"
    headings = soup.find_all(["h2", "h3"])
    target_sections = []
    for h in headings:
        ht = clean_text(h.get_text())
        if re.search(r"(what you('|’)ll learn|skills you('|’)ll gain|skills you will gain)", ht, re.I):
            # grab following sibling list(s)
            # try nearest ul/ol under the same parent or next sibling
            parent = h.parent
            for lst in parent.find_all(["ul", "ol"], limit=3):
                target_sections.append(lst)
            sib = h.find_next_sibling()
            if sib:
                for lst in sib.find_all(["ul", "ol"], limit=3):
                    target_sections.append(lst)

    for lst in target_sections:
        for li in lst.find_all("li"):
            item = clean_text(li.get_text())
            if item and item not in concepts:
                concepts.append(item)

    # Strategy B: skill chips (often spans/divs in a chips container)
    if not concepts:
        for chip in soup.select('span, div'):
            txt = clean_text(chip.get_text())
            # keep short-ish items that look like skills
            if 1 < len(txt.split()) <= 6 and any(k in txt.lower() for k in [
                "python", "java", "c++", "c ", "html", "css", "sql",
                "data", "oop", "api", "algorithms", "arrays", "pandas",
                "django", "spring", "react", "dom", "selectors", "queries",
                "object-oriented", "inheritance", "polymorphism", "encapsulation",
                "debug", "testing", "performance", "asynchronous", "promises"
            ]):
                if txt not in concepts:
                    concepts.append(txt)

    # Deduplicate & cap extremely long lists
    dedup = []
    seen = set()
    for c in concepts:
        if c not in seen:
            seen.add(c)
            dedup.append(c)
    concepts = dedup[:30]  # avoid runaway lists

    return duration, level, concepts

# =========================
# MAIN SCRAPER
# =========================
def scrape_language(driver, language: str) -> List[Dict[str, str]]:
    print(f"\n🔎 Searching Coursera for: {language}")
    driver.get(SEARCH_URL.format(q=language.replace(" ", "+")))
    wait_body(driver)
    jitter(*SCROLL_PAUSE)

    links = collect_all_course_cards(driver)
    print(f"   • Found {len(links)} course links for {language}")

    data = []
    seq = 1
    seen_links: Set[str] = set()

    for title, link in links:
        if link in seen_links:
            continue
        seen_links.add(link)

        # Visit detail page with retries
        tries = 0
        while tries <= RETRY_DETAIL:
            try:
                driver.get(link)
                wait_body(driver)
                jitter(*DETAIL_PAUSE)

                duration, level, concepts = parse_course_detail(driver)

                row = {
                    "course_id": f"C{len(data)+1:04d}",  # per-language temp; we'll resequence globally later
                    "programming_language": language,
                    "course_name": clean_text(title),
                    "course_duration": duration or "N/A",
                    "concepts": "; ".join(concepts) if concepts else "N/A",
                    "course_level": level or "N/A",
                    "course_link": link
                }
                data.append(row)
                duration_status = f"Duration: {duration}" if duration else "Duration: N/A"
                print(f"   ✅ {title} ({duration_status})")
                break
            except (TimeoutException, WebDriverException) as e:
                tries += 1
                print(f"   ↻ Retry {tries}/{RETRY_DETAIL} for: {title} ({str(e)[:80]}...)")
                jitter(2.0, 3.5)
            except Exception as e:
                print(f"   ⚠️ Skipped {title}: {e}")
                break

        # polite pacing
        jitter(0.8, 1.6)

    return data

def main():
    driver = build_driver()
    all_rows: List[Dict[str, str]] = []
    try:
        for lang in LANGUAGES:
            lang_rows = scrape_language(driver, lang)
            all_rows.extend(lang_rows)
    finally:
        driver.quit()

    # Resequence course_id globally across all languages
    for i, row in enumerate(all_rows, start=1):
        row["course_id"] = f"C{i:05d}"

    df = pd.DataFrame(
        all_rows,
        columns=[
            "course_id",
            "programming_language",
            "course_name",
            "course_duration",
            "concepts",
            "course_level",
            "course_link",
        ],
    )
    
    # Try to save CSV with error handling
    csv_filename = "courses.csv"
    try:
        df.to_csv(csv_filename, index=False, encoding="utf-8")
        print(f"\n✅ Saved {len(df)} rows to {csv_filename}")
    except PermissionError:
        # If the file is locked (open in Excel), try with a timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"courses_{timestamp}.csv"
        try:
            df.to_csv(backup_filename, index=False, encoding="utf-8")
            print(f"\n✅ Original file was locked. Saved {len(df)} rows to {backup_filename}")
            print(f"💡 Please close {csv_filename} if it's open in Excel and rename {backup_filename} to {csv_filename}")
        except Exception as e:
            print(f"\n❌ Error saving CSV file: {e}")
            print("📊 Data preview:")
            print(df.head())
    except Exception as e:
        print(f"\n❌ Unexpected error saving CSV: {e}")
        print("📊 Data preview:")
        print(df.head())

if __name__ == "__main__":
    main()
