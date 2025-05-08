# parallel_main.py

import os
import csv
import yaml
import logging
from mpi4py import MPI
from scraper.core import scrape_trending, parse_trending_cards, scrape_repo_page, parse_repo_detail
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import load_checkpoint, save_checkpoint, merge_reports, save_report
from scraper.urlgen import generate_trending_urls

_CFG_PATH = os.path.join(os.path.dirname(__file__), "config.yaml")
with open(_CFG_PATH, "r") as _f:
    _CFG = yaml.safe_load(_f)
logging.basicConfig(level=getattr(logging, _CFG["logging"]["level"]))

# filters:
_LANGUAGES = _CFG["trending"]["languages"]
_PERIODS = _CFG["trending"]["periods"]
_SPOKEN_LANGUAGES = _CFG["trending"]["spoken_languages"]

# Paths
_CP_PATH = _CFG["paths"]["checkpoint"]
_OUT_CSV = _CFG["paths"]["parallel_csv"]
_METRICS_JSON = os.path.join(_CFG["paths"]["metrics_dir"], "parallel_metrics.json")

_MAX_RETRIES = _CFG["scraper"]["max_retries"]

_ALL_URLS = generate_trending_urls(_LANGUAGES, _PERIODS, _SPOKEN_LANGUAGES)


def master(comm, size):
    logger = setup(verbose=False)
    os.makedirs(os.path.dirname(_CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(_OUT_CSV), exist_ok=True)
    os.makedirs(os.path.dirname(_METRICS_JSON), exist_ok=True)

    meta, pending = load_checkpoint(_ALL_URLS, _CP_PATH)
    total = len(_ALL_URLS)
    logger.info(f"[master] {len(meta['completed'])} done; {len(pending)} of {total} pending")

    if len(pending) == total and os.path.exists(_OUT_CSV):
        logger.info("[master] Fresh run detected: deleting existing CSV.")
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

    # dispatch initial URLs
    workers = list(range(1, size))
    for w in workers:
        url = pending.pop(0) if pending else None
        comm.send(url, dest=w, tag=1)

    t0 = MPI.Wtime()
    all_reports = []
    closed = 0

    while closed < len(workers):
        url, cards, report = comm.recv(source=MPI.ANY_SOURCE, tag=2)
        worker_rank = report['worker']

        if url is None:
            closed += 1
            all_reports.append(report)
            continue

        # write out rows
        for record in cards:
            record.pop('repo_url', None)
            writer.writerow(record)
        csv_file.flush()
        logger.info(f"[master] Added {len(cards)} rows for {url}")

        # checkpoint
        meta['completed'].append(url)
        save_checkpoint(meta, _CP_PATH)

        all_reports.append(report)

        # send next URL
        next_url = pending.pop(0) if pending else None
        comm.send(next_url, dest=worker_rank, tag=1)

    csv_file.close()

    duplicates_removed = dedupe_and_sort_csv(_OUT_CSV, sort_by=["source_url"], dedupe_on="slug")

    # merge metrics and inject duplicates_removed
    combined = merge_reports(all_reports)
    combined["duplicates_removed"] = duplicates_removed
    combined["mpi_time_s"] = MPI.Wtime() - t0

    save_report(combined, _METRICS_JSON)
    logger.info(f"[master] Complete in {combined['mpi_time_s']:.2f}s; metrics saved.")
    logger.info(f"Combined Metrics: {combined}")


def worker(comm):
    rank = comm.Get_rank()
    metrics = Metrics()
    logger = setup(verbose=False)

    while True:
        url = comm.recv(source=0, tag=1)
        if url is None:
            break

        metrics.incr('urls_total')
        logger.info(f"[rank {rank}] Fetching {url}")
        try:
            with metrics.time_block():
                # 1) trending list
                html = scrape_trending(url, max_retries=_MAX_RETRIES, metrics=metrics)
                cards = parse_trending_cards(html, source_url=url)
                # 2) detail-page pass
                enriched = []
                for c in cards:
                    repo_html = scrape_repo_page(c["repo_url"], metrics=metrics)
                    details = parse_repo_detail(repo_html, c["repo_url"])
                    enriched.append({**c, **details})
                cards = enriched

            metrics.incr('urls_success')

        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"[rank {rank}] Error fetching {url}: {e}")
            cards = []

        report = metrics.report()
        report['worker'] = rank
        comm.send((url, cards, report), dest=0, tag=2)

    final_report = metrics.report()
    final_report['worker'] = rank
    comm.send((None, None, final_report), dest=0, tag=2)


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        master(comm, size)
    else:
        worker(comm)


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
