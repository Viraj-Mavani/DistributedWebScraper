# serial_main.py
import logging
import csv
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import save_report

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    urls = [
        'https://github.com/trending?since=daily',
        'https://github.com/trending?since=weekly',
        'https://github.com/trending/python?since=daily',
        'https://github.com/trending/python?since=weekly',
    ]

    flat = []
    for url in urls:
        metrics.incr('urls_total')
        logger.info(f"Fetching {url}")
        try:
            items = scrape_trending(url, max_retries=3, metrics=metrics)
            metrics.incr('urls_success')
            for pos, repo in items:
                flat.append({'url': url, 'position': pos, 'repo': repo})
        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"Error fetching {url} after retries: {e}")
            continue

    out_path = 'data/output/trending_serial.csv'
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
        writer.writeheader()
        writer.writerows(flat)
    logger.info(f"Wrote {len(flat)} rows to {out_path}")

    metrics_path = 'metrics/serial_metrics.json'
    save_report(metrics.report(), metrics_path)
    logger.info(f"Serial run metrics saved to {metrics_path}")
    logger.info(f"Metrics: {metrics.report()}")

if __name__ == '__main__':
    main()
