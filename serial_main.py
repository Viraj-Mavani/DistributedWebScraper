import logging
import csv
import time
import requests
from bs4 import BeautifulSoup
from scraper.metrics import Metrics
from scraper.logger import setup


def scrape_trending(url):
    """Fetch and parse a GitHub Trending page, returning a list of repo names."""
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    return [
        card.select_one('h2 a')
            .get_text(strip=True)
            .replace(' / ', '/')
        for card in soup.select('article.Box-row')
    ]


def main():
    # Configure logging and metrics
    logger = setup(verbose=False)
    metrics = Metrics()

    # URLs to scrape
    urls = [
        'https://github.com/trending?since=daily',
        'https://github.com/trending?since=weekly',
        'https://github.com/trending/python?since=daily',
        'https://github.com/trending/python?since=weekly',
    ]

    all_data = []
    start_time = time.time()

    for url in urls:
        logger.info(f"Fetching {url}")
        metrics.incr('urls_total')
        with metrics.time_block():
            try:
                repos = scrape_trending(url)
                metrics.incr('urls_success')
                for pos, repo in enumerate(repos, start=1):
                    all_data.append({'url': url, 'position': pos, 'repo': repo})
            except requests.RequestException as e:
                metrics.incr('urls_failed')
                logger.warning(f"Fetch failed for {url}: {e}")
            except Exception as e:
                metrics.incr('parse_errors')
                logger.error(f"Error parsing {url}: {e}")

    duration = time.time() - start_time

    # Write results to CSV
    out_path = 'data/output/trending_serial.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
        writer.writeheader()
        writer.writerows(all_data)
    logger.info(f"Wrote {len(all_data)} rows to {out_path}")

    # Save metrics
    metrics.save('metrics/serial_metrics.json')
    logger.info(f"Serial run duration: {duration:.2f}s")
    logger.info(f"Metrics: {metrics.report()}")


if __name__ == '__main__':
    main()
