import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Tuple

def scrape_trending(
    url: str,
    max_retries: int = 3,
    metrics = None
) -> List[Tuple[int, str]]:
    """
    Fetch and parse a GitHub Trending page, retrying up to max_retries.
    If provided, increments 'retries' in the metrics on each retry attempt.

    :param url: Full URL of the GitHub Trending endpoint.
    :param max_retries: Number of total attempts before giving up.
    :param metrics: Optional Metrics instance for retry counting.
    :return: List of (position, full_repo_name).
    """
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, 'html.parser')

            results = []
            for idx, card in enumerate(soup.select('article.Box-row'), start=1):
                name = card.select_one('h2 a').get_text(strip=True).replace(' / ', '/')
                results.append((idx, name))
            return results
        except Exception as e:
            last_exc = e
            if metrics is not None:
                metrics.incr('retries')
            if attempt == max_retries:
                raise
    # Fallback: raise last exception if somehow loop exits
    raise last_exc
