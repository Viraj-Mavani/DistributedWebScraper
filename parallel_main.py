import logging
import csv
from mpi4py import MPI
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import chunk_urls, merge_reports, save_report

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # master builds job list
    if rank == 0:
        urls = [
            'https://github.com/trending?since=daily',
            'https://github.com/trending?since=weekly',
            'https://github.com/trending/python?since=daily',
            'https://github.com/trending/python?since=weekly',
        ]
        jobs = chunk_urls(urls, size)
    else:
        jobs = None

    my_jobs = comm.scatter(jobs, root=0)
    local_rows = []

    t0 = MPI.Wtime()
    for url in my_jobs:
        metrics.incr('urls_total')
        logger.info(f"[rank {rank}] Fetching {url}")
        with metrics.time_block():
            try:
                items = scrape_trending(url)
                metrics.incr('urls_success')
            except Exception as e:
                metrics.incr('urls_failed')
                logger.warning(f"[rank {rank}] Error {url}: {e}")
                continue

        for pos, repo in items:
            local_rows.append({'url': url, 'position': pos, 'repo': repo})
        logger.info(f"[rank {rank}] scraped {len(items)} items from {url}")

    # gather CSV rows and metrics
    all_rows    = comm.gather(local_rows,    root=0)
    all_reports = comm.gather(metrics.report(), root=0)

    if rank == 0:
        # write combined CSV
        flat = [r for sub in all_rows for r in sub]
        out_path = 'data/output/trending_parallel.csv'
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
            writer.writeheader()
            writer.writerows(flat)
        logger.info(f"Wrote {len(flat)} rows to {out_path}")

        # combine and save metrics
        combined = merge_reports(all_reports)
        total_time = MPI.Wtime() - t0
        combined['mpi_time_s'] = total_time
        metrics_path = 'metrics/parallel_metrics.json'
        save_report(combined, metrics_path)
        logger.info(f"Parallel run time: {total_time:.2f}s")
        logger.info(f"Combined Metrics: {combined}")

if __name__ == '__main__':
    main()
