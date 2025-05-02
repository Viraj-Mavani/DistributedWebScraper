# parallel_main.py
import os, logging, csv
from mpi4py import MPI
from scraper.core import scrape_trending
from scraper.metrics import Metrics
from scraper.logger import setup
from scraper.scheduler import merge_reports, save_report

DONE_FILE = 'data/output/done_urls.txt'
OUT_CSV   = 'data/output/trending_parallel.csv'

def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Rank 0: load checkpoint, auto-reset if needed
    if rank == 0:
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
        # load/checkpoint
        done = set()
        if os.path.exists(DONE_FILE):
            done = {u.strip() for u in open(DONE_FILE)}
        # if fully done, reset both files
        if done == set(ALL_URLS):
            logger.info("All URLs done; resetting CSV & checkpoint.")
            try: os.remove(DONE_FILE)
            except FileNotFoundError: pass
            try: os.remove(OUT_CSV)
            except FileNotFoundError: pass
            done.clear()
        # header writing if fresh
        if not os.path.exists(OUT_CSV):
            with open(OUT_CSV, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
                writer.writeheader()
        pending = [u for u in ALL_URLS if u not in done]
        logger.info(f"[rank 0] {len(done)} done; {len(pending)} pending")
        jobs = [pending[i::size] for i in range(size)]
    else:
        # other ranks wait for job list
        jobs = None

    # Synchronize header write
    jobs = comm.bcast(jobs, root=0)

    # Split work
    my_jobs = jobs[rank]
    t0 = MPI.Wtime()

    # No barriers inside the loop!
    for url in my_jobs:
        metrics.incr('urls_total')
        logger.info(f"[rank {rank}] Fetching {url}")
        try:
            items = scrape_trending(url, max_retries=3, metrics=metrics)
            metrics.incr('urls_success')
            # Append rows immediately
            with open(OUT_CSV, 'a', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
                for pos, repo in items:
                    writer.writerow({'url':url,'position':pos,'repo':repo})
            # Append checkpoint immediately
            with open(DONE_FILE, 'a', encoding='utf-8') as f:
                f.write(url + "\n")
            logger.info(f"[rank {rank}] done {url} ({len(items)} rows)")
        except Exception as e:
            metrics.incr('urls_failed')
            logger.warning(f"[rank {rank}] fail {url}: {e}")

    # Gather metrics only
    all_reports = comm.gather(metrics.report(), root=0)

    if rank == 0:
        # No need to gather rows—the CSV is already complete
        combined = merge_reports(all_reports)
        combined['mpi_time_s'] = MPI.Wtime() - t0  # optional, or track before loop
        metrics_path = 'metrics/parallel_metrics.json'
        save_report(combined, metrics_path)
        logger.info(f"Parallel run time: {combined['mpi_time_s']:.2f}s")
        logger.info(f"Combined Metrics: {combined}")

if __name__ == '__main__':
    main()
