import requests
from bs4 import BeautifulSoup


def scrape_trending(url: str):
    """
    Fetch and parse a GitHub Trending page, returning a list of (position, repo_name).

    :param url: Full URL of the GitHub Trending endpoint to scrape.
    :return: List of tuples (rank, full_repo_name).
    """
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')

    results = []
    cards = soup.select('article.Box-row')
    for idx, card in enumerate(cards, start=1):
        name_tag = card.select_one('h2 a')
        full_name = name_tag.get_text(strip=True).replace(' / ', '/')
        results.append((idx, full_name))
    return results
