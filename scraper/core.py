# scraper/core.py

import os
import random
import time
import yaml
import cloudscraper
from bs4 import BeautifulSoup
import logging

_CFG_PATH = os.path.join(os.path.dirname(__file__), os.pardir, "config.yaml")
with open(_CFG_PATH, "r") as _f:
    _CFG = yaml.safe_load(_f)

# pull scraper settings
_TIMEOUT         = _CFG["scraper"]["timeout"]
_MAX_RETRIES     = _CFG["scraper"]["max_retries"]
_USER_AGENT      = _CFG["scraper"]["user_agent"].strip()
_ACCEPT_LANGUAGE = _CFG["scraper"]["accept_language"].strip()
_REFERER         = _CFG["scraper"]["referer"].strip()
_SLEEP_MIN       = _CFG["scraper"]["sleep_between_requests"]["min"]
_SLEEP_MAX       = _CFG["scraper"]["sleep_between_requests"]["max"]

# cloudscraper will handle any CF/UAM challenges
_CS = cloudscraper.create_scraper()
logger = logging.getLogger(__name__)

def scrape_trending(url: str, max_retries: int = None, metrics=None) -> str:
    """
    Fetch the raw HTML of a GitHub Trending page via cloudscraper (so CF/UAM
    challenges are handled), with retry logic and a small random delay.
    """
    attempt = 0
    retries = max_retries if max_retries is not None else _MAX_RETRIES

    while True:
        try:
            headers = {
                "User-Agent":      _USER_AGENT,
                "Accept-Language": _ACCEPT_LANGUAGE,
                "Referer":         _REFERER,
            }
            time.sleep(random.uniform(_SLEEP_MIN, _SLEEP_MAX))
            resp = _CS.get(url, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp.text

        except Exception:
            attempt += 1
            if metrics:
                metrics.incr("retries")
            if attempt >= retries:
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


def scrape_repo_page(repo_url: str, max_retries: int = None, metrics=None) -> str:
    """
    Fetch a repo’s main page (uses the same headers + a small random pause).
    """
    attempt = 0
    retries = max_retries if max_retries is not None else _MAX_RETRIES

    while True:
        try:
            headers = {
                "User-Agent":      _USER_AGENT,
                "Accept-Language": _ACCEPT_LANGUAGE,
                "Referer":         _REFERER,
            }
            time.sleep(random.uniform(_SLEEP_MIN, _SLEEP_MAX))
            resp = _CS.get(repo_url, headers=headers, timeout=_TIMEOUT)
            resp.raise_for_status()
            return resp.text
        except Exception:
            attempt += 1
            if metrics:
                metrics.incr("retries")
            if attempt >= retries:
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

    # --- top contributors usernames ---
    top_contributors = []
    if contributors_count > 0:
        # locate the exact “Contributors” sidebar cell
        contrib_cell = None
        for div in soup.select("div.BorderGrid-cell"):
            if div.select_one('a[href$="/graphs/contributors"].Link--primary'):
                contrib_cell = div
                break

        if contrib_cell:
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