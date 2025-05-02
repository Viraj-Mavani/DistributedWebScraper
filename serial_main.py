# serial_main.py
import logging
import csv
import os
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import save_report

# Paths for checkpoint and output
DONE_FILE = 'data/output/done_urls.txt'
OUT_CSV   = 'data/output/trending_serial.csv'
# Full list of trending endpoints
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

    # Ensure output directory exists
    os.makedirs(os.path.dirname(DONE_FILE), exist_ok=True)

    # Load checkpoint
    if os.path.exists(DONE_FILE):
        with open(DONE_FILE, 'r', encoding='utf-8') as f:
            done = {line.strip() for line in f if line.strip()}
    else:
        done = set()

    # Auto-reset if done covers all URLs
    if done == set(ALL_URLS):
        logger.info("All URLs already processed; resetting checkpoint and CSV for fresh run.")
        try:
            os.remove(DONE_FILE)
        except FileNotFoundError:
            pass
        try:
            os.remove(OUT_CSV)
        except FileNotFoundError:
            pass
        done.clear()

    # Determine pending URLs
    pending = [u for u in ALL_URLS if u not in done]
    total = len(ALL_URLS)
    logger.info(f"{len(done)} URLs already done; {len(pending)} of {total} to process")

    if not pending:
        logger.info("No pending URLs - exiting.")
        return

    # Prepare CSV file
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
            # Mark done
            with open(DONE_FILE, 'a', encoding='utf-8') as f:
                f.write(url + "\n")
            done.add(url)
        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"Error fetching {url} after retries: {e}")

    csv_file.close()

    # Save metrics output
    metrics_path = 'metrics/serial_metrics.json'
    save_report(metrics.report(), metrics_path)
    logger.info(f"Serial run metrics saved to {metrics_path}")
    logger.info(f"Metrics: {metrics.report()}")

if __name__ == '__main__':
    main()
