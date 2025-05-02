# serial_main.py

import logging
import csv
import os
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import load_checkpoint, save_checkpoint, save_report

# Paths
CP_PATH = 'data/output/checkpoint.json'
OUT_CSV = 'data/output/trending_serial.csv'

# Your full list of endpoints
ALL_URLS = [
    'https://github.com/trending?since=daily',
    'https://github.com/trending?since=weekly',
    'https://github.com/trending?since=monthly',
    'https://github.com/trending/python?since=daily',
    'https://github.com/trending/python?since=weekly',
    'https://github.com/trending/python?since=monthly',
    'https://github.com/trending/javascript?since=daily',
    'https://github.com/trending/javascript?since=weekly',
    'https://github.com/trending/javascript?since=monthly',
]

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    # Ensure output directories
    os.makedirs(os.path.dirname(CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    # Load or reset checkpoint
    meta, pending = load_checkpoint(ALL_URLS, CP_PATH)
    total = len(ALL_URLS)
    logger.info(f"{len(meta['completed'])} done; {len(pending)} of {total} pending")

    if not pending:
        logger.info("No pending URLs - exiting.")
        return

    if len(pending) == total and os.path.exists(OUT_CSV):
        logger.info("Fresh run detected: deleting existing CSV.")
        os.remove(OUT_CSV)

    # Open CSV (append mode), write header if new
    new_csv = not os.path.exists(OUT_CSV)
    csv_file = open(OUT_CSV, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=['url','position','repo'])
    if new_csv:
        writer.writeheader()
        csv_file.flush()

    # Process each pending URL
    for url in pending:
        metrics.incr('urls_total')
        logger.info(f"Fetching {url}")
        try:
            items = scrape_trending(url, max_retries=3, metrics=metrics)
            metrics.incr('urls_success')
            # Write rows immediately
            for pos, repo in items:
                writer.writerow({'url': url, 'position': pos, 'repo': repo})
            csv_file.flush()
            # Mark done in checkpoint and save
            meta['completed'].append(url)
            save_checkpoint(meta, CP_PATH)
        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"Error fetching {url} after retries: {e}")

    csv_file.close()

    # Save metrics
    metrics_path = 'metrics/serial_metrics.json'
    save_report(metrics.report(), metrics_path)
    logger.info(f"Serial run metrics saved to {metrics_path}")
    logger.info(f"Metrics: {metrics.report()}")

if __name__ == '__main__':
    main()
