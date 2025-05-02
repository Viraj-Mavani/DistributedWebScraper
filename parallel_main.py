# parallel_main.py

import os
import logging
import csv
from mpi4py import MPI
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import (
    load_checkpoint,
    save_checkpoint,
    merge_reports,
    save_report,
)

# Paths
CP_PATH = 'data/output/checkpoint.json'
OUT_CSV = 'data/output/trending_parallel.csv'

# Full list of endpoints
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

def master(comm, size):
    logger = setup(False)
    os.makedirs(os.path.dirname(CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    # Load/reset checkpoint and CSV
    meta, pending = load_checkpoint(ALL_URLS, CP_PATH)
    total = len(ALL_URLS)
    logger.info(f"[master] {len(meta['completed'])} done; {len(pending)} of {total} pending")

    # Fresh-run deletion
    if len(pending) == total and os.path.exists(OUT_CSV):
        logger.info("[master] Fresh run detected: deleting existing CSV.")
        os.remove(OUT_CSV)

    # Open CSV once, write header if new
    new_csv = not os.path.exists(OUT_CSV)
    csv_file = open(OUT_CSV, 'a', newline='', encoding='utf-8')
    writer = csv.DictWriter(csv_file, fieldnames=['url','position','repo'])
    if new_csv:
        writer.writeheader()
        csv_file.flush()

    # Dynamic dispatch
    workers = list(range(1, size))
    closed = 0
    # initial dispatch: one URL per worker
    for w in workers:
        if pending:
            url = pending.pop(0)
        else:
            url = None
        comm.send(url, dest=w, tag=1)

    t0 = MPI.Wtime()
    all_reports = []

    # Loop until all workers signal done
    while closed < len(workers):
        # Receive a result or done signal
        url, items, report = comm.recv(source=MPI.ANY_SOURCE, tag=2)
        w = report.pop('rank', None)  # we can inject rank in worker if needed
        # If url is None, that worker is done
        if url is None:
            closed += 1
            all_reports.append(report)
            continue

        # Write its rows
        for pos, repo in items:
            writer.writerow({'url': url, 'position': pos, 'repo': repo})
        logger.info(f"[master] Added {len(items)} rows for {url}")
        csv_file.flush()

        # Update checkpoint
        meta['completed'].append(url)
        save_checkpoint(meta, CP_PATH)

        # Store its metrics (without rank) for later merge
        all_reports.append(report)

        # Dispatch next URL
        if pending:
            next_url = pending.pop(0)
        else:
            next_url = None
        comm.send(next_url, dest=report.get('worker'), tag=1)

    csv_file.close()

    # Merge all metrics and save
    combined = merge_reports(all_reports)
    combined['mpi_time_s'] = MPI.Wtime() - t0
    save_report(combined, 'metrics/parallel_metrics.json')
    logger.info(f"[master] Complete in {combined['mpi_time_s']:.2f}s; metrics saved.")
    logger.info(f"Combined Metrics: {combined}")

def worker(comm):
    rank = comm.Get_rank()
    metrics = Metrics()
    logger = setup(False)

    while True:
        url = comm.recv(source=0, tag=1)
        # None signals shutdown
        if url is None:
            break

        metrics.incr('urls_total')
        logger.info(f"[rank {rank}] Fetching {url}")
        try:
            items = scrape_trending(url, max_retries=3, metrics=metrics)
            metrics.incr('urls_success')
        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"[rank {rank}] Error {url}: {e}")
            items = []

        # Send back (url, items, report+worker id)
        report = metrics.report()
        report['worker'] = rank
        comm.send((url, items, report), dest=0, tag=2)

    # Final callback with (None) to indicate done
    report = metrics.report()
    report['worker'] = rank
    comm.send((None, None, report), dest=0, tag=2)

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    if rank == 0:
        master(comm, size)
    else:
        worker(comm)

if __name__ == '__main__':
    main()
