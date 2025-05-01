import time
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Tuple
from scraper.metrics import Metrics


def scrape_trending(
    url: str,
    max_retries: int = 3,
    metrics: Optional[Metrics] = None
) -> List[Tuple[int, str]]:
    """
    Fetch and parse a GitHub Trending page with retry and timing support.

    :param url: The full GitHub Trending URL to scrape.
    :param max_retries: Number of attempts before giving up.
    :param metrics: Optional Metrics instance; if provided,
                    'retries' counter and per-URL timing will be recorded.
    :return: List of tuples (position, full_repo_name).
    :raises: Last exception if all retries fail.
    """
    for attempt in range(1, max_retries + 1):
        if attempt > 1 and metrics is not None:
            metrics.incr('retries')
        try:
            # Time the fetch+parse if metrics provided
            if metrics is not None:
                with metrics.time_block():
                    resp = requests.get(url, timeout=10)
                    resp.raise_for_status()
                    html = resp.text
            else:
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                html = resp.text

            soup = BeautifulSoup(html, 'html.parser')
            cards = soup.select('article.Box-row')
            result: List[Tuple[int, str]] = []
            for idx, card in enumerate(cards, start=1):
                name = card.select_one('h2 a').get_text(strip=True).replace(' / ', '/')
                result.append((idx, name))
            return result

        except Exception:
            # On final attempt, propagate exception
            if attempt == max_retries:
                raise
            # Otherwise, retry
    # Should never reach here
    return []
