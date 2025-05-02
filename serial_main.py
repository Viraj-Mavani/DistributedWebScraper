# serial_main.py

import logging
import csv
import os
from scraper.core import scrape_trending, parse_trending_cards
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import load_checkpoint, save_checkpoint, save_report
from scraper.urlgen import generate_trending_urls

# filters:
LANGUAGES = ['Python','JavaScript']
PERIODS   = ['daily','weekly','monthly']
SPOKEN_LANGUAGES = ['','en']

# Paths
CP_PATH = 'data/output/checkpoint.json'
OUT_CSV  = 'data/output/trending_serial.csv'

ALL_URLS = generate_trending_urls(LANGUAGES, PERIODS, SPOKEN_LANGUAGES)

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    os.makedirs(os.path.dirname(CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    meta, pending = load_checkpoint(ALL_URLS, CP_PATH)
    total = len(ALL_URLS)
    logger.info(f"{len(meta['completed'])} done; {len(pending)} of {total} pending")

    if not pending:
        logger.info("No pending URLs - exiting.")
        return

    if len(pending) == total and os.path.exists(OUT_CSV):
        logger.info("Fresh run detected—deleting existing CSV.")
        os.remove(OUT_CSV)

    fieldnames = [
        'source_url','position','slug','owner','repo',
        'description','language','stars','stars_today','forks'
    ]

    new_csv = not os.path.exists(OUT_CSV)
    csv_file = open(OUT_CSV, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    if new_csv:
        writer.writeheader()
        csv_file.flush()

    for url in pending:
        metrics.incr('urls_total')
        logger.info(f"Fetching {url}")
        try:
            with metrics.time_block():
                html  = scrape_trending(url, max_retries=3, metrics=metrics)
                cards = parse_trending_cards(html, source_url=url)
            metrics.incr('urls_success')

            for record in cards:
                writer.writerow(record)
            csv_file.flush()

            meta['completed'].append(url)
            save_checkpoint(meta, CP_PATH)

        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"Error fetching {url} after retries: {e}")

    csv_file.close()

    save_report(metrics.report(), 'metrics/serial_metrics.json')
    logger.info("Serial run metrics saved.")
    logger.info(f"Metrics: {metrics.report()}")


if __name__ == '__main__':
    main()
