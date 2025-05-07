# scraper/core.py
import random
import time
import cloudscraper
import requests
from bs4 import BeautifulSoup
import logging

# cloudscraper will handle any CF/UAM challenges
_CS = cloudscraper.create_scraper()
logger = logging.getLogger(__name__)

# a slightly larger pool of real-world User-Agent headers
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.0.0 Safari/537.36"
)

def scrape_trending(url: str, max_retries: int = 3, metrics=None) -> str:
    """
    Fetch the raw HTML of a GitHub Trending page via cloudscraper (so CF/UAM
    challenges are handled), with retry logic and a small random delay.
    Returns the page HTML as text.
    """
    attempt = 0
    while True:
        try:
            headers = {
                "User-Agent": DESKTOP_UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://github.com/",
            }
            time.sleep(random.uniform(0.5, 1))
            resp = _CS.get(url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.text
        except Exception:
            attempt += 1
            if metrics:
                metrics.incr("retries")
            if attempt >= max_retries:
                raise
            time.sleep(1)


def parse_trending_cards(html: str, source_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("article.Box-row")
    results = []

    for idx, card in enumerate(cards, start=1):
        href = card.h2.a["href"].lstrip("/")       # e.g. "owner/repo"
        owner, repo = href.split("/", 1)
        repo_url = f"https://github.com/{href}"

        # description
        desc_el = card.select_one("p.col-9")
        description = desc_el.text.strip() if desc_el else ""

        # primary language
        lang_el = card.select_one("[itemprop=programmingLanguage]")
        language = lang_el.text.strip() if lang_el else ""

        # total stars
        stars = 0
        star_el = card.select_one(f'a[href="/{href}/stargazers"]')
        if star_el and star_el.text:
            try:
                stars = int(star_el.text.strip().replace(",", ""))
            except ValueError:
                pass

        # stars this period
        stars_today_el = card.select_one(".float-sm-right")
        stars_today = stars_today_el.text.strip() if stars_today_el else ""

        # forks
        forks = 0
        forks_el = card.select_one(f'a[href="/{href}/forks"]')
        if forks_el and forks_el.text:
            try:
                forks = int(forks_el.text.strip().replace(",", ""))
            except ValueError:
                pass

        results.append({
            "source_url":  source_url,
            "position":    idx,
            "slug":        href,
            "owner":       owner,
            "repo":        repo,
            "repo_url":    repo_url,
            "description": description,
            "language":    language,
            "stars":       stars,
            "stars_today": stars_today,
            "forks":       forks,
        })

    return results


def scrape_repo_page(repo_url: str, max_retries: int = 3, metrics=None) -> str:
    attempt = 0
    while True:
        try:
            headers = {
                "User-Agent": DESKTOP_UA,
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://github.com/",
            }
            time.sleep(random.uniform(0.5, 1.5))
            resp = _CS.get(repo_url, headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.text
        except Exception:
            attempt += 1
            if metrics:
                metrics.incr("retries")
            if attempt >= max_retries:
                raise
            time.sleep(1)


def parse_repo_detail(html: str, repo_url: str, metrics=None) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # --- license ---
    license_name = ""
    lic_el = soup.select_one('a[title*="License"], a[href$="/LICENSE"], a[href*="/blob/master/LICENSE"]')
    if lic_el:
        license_name = lic_el.get_text(strip=True)

    # --- open issues ---
    open_issues = 0
    issues_el = soup.select_one('a[href$="/issues"] .Counter')
    if issues_el:
        raw = issues_el.get("title", issues_el.get_text(strip=True))
        try:
            open_issues = int(raw.replace(",", ""))
        except ValueError:
            if metrics:
                metrics.incr("parse_errors")
            logger.warning(f"[parse] could not parse open_issues ({raw!r}) on {repo_url}")

    # --- contributors count & list ---
    contributors_count = 0
    contrib_link = soup.select_one('a[href$="/graphs/contributors"].Link--primary')
    if contrib_link:
        counter_el = contrib_link.select_one(".Counter")
        raw = (counter_el.get("title") or counter_el.get_text(strip=True)).strip()
        try:
            contributors_count = int(raw.lstrip("+").replace(",", ""))
        except ValueError:
            if metrics:
                metrics.incr("parse_errors")
            logger.warning(f"[parse] could not parse contributors_count on {repo_url}")

    # --- top contributors usernames (from the avatar list) ---
    top_contributors = []
    if contributors_count > 0:
        # 1) find the exact “Contributors” sidebar cell
        contrib_cell = None
        for div in soup.select("div.BorderGrid-cell"):
            if div.select_one('a[href$="/graphs/contributors"].Link--primary'):
                contrib_cell = div
                break

        if contrib_cell:
            # 2) within that cell, look for the avatars-list UL
            avatar_list = contrib_cell.select_one("ul.list-style-none.d-flex.flex-wrap.mb-n2")
            if avatar_list:
                items = avatar_list.select("li a")
                # logger.info(f"[parse] found {len(items)} top contributors for {repo_url}")
                if items:
                    for a in items:
                        href = a.get("href", "")
                        if href:
                            top_contributors.append(href.rstrip("/").split("/")[-1])

        else:
            logger.warning(f"[parse] could not locate Contributors cell on {repo_url}")

    logger.info(f"[parse] {repo_url} - Done")


    return {
        "license":            license_name,
        "open_issues":        open_issues,
        "contributors_count": contributors_count,
        "top_contributors":   top_contributors,
    }