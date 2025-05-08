# serial_main.py

import yaml
import logging
import csv
import os
from scraper.core import scrape_trending, parse_trending_cards, scrape_repo_page, parse_repo_detail
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import load_checkpoint, save_checkpoint, save_report
from scraper.urlgen import generate_trending_urls

_CFG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(_CFG_PATH, "r") as _f:
    _CFG = yaml.safe_load(_f)
logging.basicConfig(level=getattr(logging, _CFG["logging"]["level"]))

# filters:
_LANGUAGES        = _CFG["trending"]["languages"]
_PERIODS          = _CFG["trending"]["periods"]
_SPOKEN_LANGUAGES = _CFG["trending"]["spoken_languages"]

# Paths
_CP_PATH        = _CFG["paths"]["checkpoint"]
_OUT_CSV        = _CFG["paths"]["serial_csv"]
_METRICS_JSON   = os.path.join(_CFG["paths"]["metrics_dir"], "serial_metrics.json")

_MAX_RETRIES  = _CFG["scraper"]["max_retries"]

_ALL_URLS = generate_trending_urls(_LANGUAGES, _PERIODS, _SPOKEN_LANGUAGES)

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    os.makedirs(os.path.dirname(_CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(_OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(_METRICS_JSON), exist_ok=True)

    meta, pending = load_checkpoint(_ALL_URLS, _CP_PATH)
    total = len(_ALL_URLS)
    logger.info(f"{len(meta['completed'])} done; {len(pending)} of {total} pending")

    if not pending:
        logger.info("No pending URLs - exiting.")
        return

    if len(pending) == total and os.path.exists(_OUT_CSV):
        logger.info("Fresh run detected—deleting existing CSV.")
        os.remove(_OUT_CSV)

    fieldnames = [
        'source_url', 'position', 'slug', 'owner', 'repo', 'description', 'language', 'stars', 'stars_today', 'forks',
        'license', 'open_issues', 'contributors_count', 'top_contributors'
    ]

    new_csv = not os.path.exists(_OUT_CSV)
    csv_file = open(_OUT_CSV, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    if new_csv:
        writer.writeheader()
        csv_file.flush()

    for url in pending:
        metrics.incr('urls_total')
        logger.info(f"Fetching {url}")
        try:
            with metrics.time_block():
                # 1) get trending list
                html  = scrape_trending(url, max_retries=_MAX_RETRIES, metrics=metrics)
                cards = parse_trending_cards(html, source_url=url)

                # 2) for each card, fan out to the repo page
                enriched = []
                for c in cards:
                    repo_html = scrape_repo_page(c["repo_url"], metrics=metrics)
                    details = parse_repo_detail(repo_html, c["repo_url"])
                    enriched.append({**c, **details})
                cards = enriched

            metrics.incr('urls_success')

            # write out all records
            for record in cards:
                record.pop('repo_url', None)
                writer.writerow(record)
            csv_file.flush()

            # update checkpoint
            meta['completed'].append(url)
            save_checkpoint(meta, _CP_PATH)

        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"Error fetching {url} after retries: {e}")

    csv_file.close()

    duplicates_removed = dedupe_and_sort_csv(_OUT_CSV, sort_by=["source_url"], dedupe_on="slug")

    report = metrics.report()
    report["duplicates_removed"] = duplicates_removed

    save_report(metrics.report(), _METRICS_JSON)
    logger.info("Serial run metrics saved.")
    logger.info(f"Metrics: {report}")


def dedupe_and_sort_csv(path: str, *, sort_by: list[str], dedupe_on: str = "slug") -> int:
    """
    Reads `path`, drops any rows whose `dedupe_on` field we've already seen,
    sorts the survivors by `sort_by`, writes them back, and returns how many
    duplicates were removed.
    """
    seen = set()
    unique_rows = []
    duplicates = 0

    # read & filter
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row[dedupe_on]
            if key in seen:
                duplicates += 1
                continue
            seen.add(key)
            unique_rows.append(row)

    # sort
    unique_rows.sort(key=lambda r: tuple(r[col] for col in sort_by))

    # write back
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(unique_rows)

    return duplicates


if __name__ == '__main__':
    main()
