# scraper/core.py

import time
import requests
from bs4 import BeautifulSoup

def scrape_trending(url: str, max_retries: int = 3, metrics=None) -> str:
    """
    Fetch the raw HTML of a GitHub Trending page, with retry logic.
    Returns the page HTML as text.

    :param url: the Trending page URL
    :param max_retries: how many times to retry on failure
    :param metrics: optional Metrics object to record retries
    """
    attempt = 0
    while True:
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            attempt += 1
            if metrics:
                metrics.incr('retries')
            if attempt >= max_retries:
                # let the caller handle/log the exception
                raise
            # small back-off before retrying
            time.sleep(1)


def parse_trending_cards(html: str, source_url: str) -> list[dict]:
    """
    Given the HTML of a GitHub Trending page and its URL,
    parse out each <article.Box-row> and return a list of dicts:
      - source_url   (the trending page we came from)
      - position     (1-based index on the page)
      - slug         ("owner/repo")
      - owner        ("owner")
      - repo         ("repo")
      - description  (text under the repo name)
      - language     (primary programming language)
      - stars        (total star count, int)
      - stars_today  (e.g. "123 stars today")
      - forks        (total fork count, int)
    """
    soup = BeautifulSoup(html, 'html.parser')
    cards = soup.select('article.Box-row')
    results = []

    for idx, card in enumerate(cards, start=1):
        # e.g. href="/owner/repo"
        href = card.h2.a['href'].lstrip('/')
        owner, repo = href.split('/', 1)

        # description
        desc_el = card.select_one('p.col-9')
        description = desc_el.text.strip() if desc_el else ''

        # primary language
        lang_el = card.select_one('[itemprop=programmingLanguage]')
        language = lang_el.text.strip() if lang_el else ''

        # total stars
        star_el = card.select_one(f'a[href="/{href}/stargazers"]')
        stars = 0
        if star_el and star_el.text:
            try:
                stars = int(star_el.text.strip().replace(',', ''))
            except ValueError:
                stars = 0

        # stars today / this period
        stars_today_el = card.select_one('.float-sm-right')
        stars_today = stars_today_el.text.strip() if stars_today_el else ''

        # total forks
        forks_el = card.select_one(f'a[href="/{href}/forks"]')
        forks = 0
        if forks_el and forks_el.text:
            try:
                forks = int(forks_el.text.strip().replace(',', ''))
            except ValueError:
                forks = 0

        results.append({
            "source_url":  source_url,
            "position":    idx,
            "slug":        href,
            "owner":       owner,
            "repo":        repo,
            "description": description,
            "language":    language,
            "stars":       stars,
            "stars_today": stars_today,
            "forks":       forks,
        })

    return results
