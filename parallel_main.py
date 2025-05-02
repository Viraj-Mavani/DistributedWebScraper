# parallel_main.py

import os
import logging
import csv
from mpi4py import MPI
from scraper.core import scrape_trending, parse_trending_cards
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import load_checkpoint, save_checkpoint, merge_reports, save_report
from scraper.urlgen import generate_trending_urls

# filters:
LANGUAGES = ['Python','JavaScript']
PERIODS   = ['daily','weekly','monthly']
SPOKEN_LANGUAGES = ['','en']

# Paths
CP_PATH = 'data/output/checkpoint.json'
OUT_CSV  = 'data/output/trending_parallel.csv'

ALL_URLS = generate_trending_urls(LANGUAGES, PERIODS, SPOKEN_LANGUAGES)

def master(comm, size):
    logger = setup(verbose=False)
    os.makedirs(os.path.dirname(CP_PATH), exist_ok=True)
    os.makedirs(os.path.dirname(OUT_CSV), exist_ok=True)

    meta, pending = load_checkpoint(ALL_URLS, CP_PATH)
    total = len(ALL_URLS)
    logger.info(f"[master] {len(meta['completed'])} done; {len(pending)} of {total} pending")

    if len(pending) == total and os.path.exists(OUT_CSV):
        logger.info("[master] Fresh run detected: deleting existing CSV.")
        os.remove(OUT_CSV)

    # Fix: include 'stars_today' instead of 'stars_current'
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

        for record in cards:
            writer.writerow(record)
        logger.info(f"[master] Added {len(cards)} rows for {url}")
        csv_file.flush()

        meta['completed'].append(url)
        save_checkpoint(meta, CP_PATH)

        all_reports.append(report)

        next_url = pending.pop(0) if pending else None
        comm.send(next_url, dest=worker_rank, tag=1)

    csv_file.close()

    combined = merge_reports(all_reports)
    combined['mpi_time_s'] = MPI.Wtime() - t0
    save_report(combined, 'metrics/parallel_metrics.json')
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
                html  = scrape_trending(url, max_retries=3, metrics=metrics)
                cards = parse_trending_cards(html, source_url=url)
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


if __name__ == '__main__':
    main()
