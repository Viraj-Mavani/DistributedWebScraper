import logging
import csv
from mpi4py import MPI
import requests
from bs4 import BeautifulSoup
from scraper.metrics import Metrics
from scraper.logger import setup


def scrape_trending(url):
    """Fetch and parse a GitHub Trending page, returning a list of (position, repo_name)."""
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    out = []
    for idx, card in enumerate(soup.select('article.Box-row'), start=1):
        name = card.select_one('h2 a').get_text(strip=True).replace(' / ', '/')
        out.append((idx, name))
    return out


def chunk_urls(urls, size):
    """Round-robin split: rank i gets urls[i::size]."""
    return [urls[i::size] for i in range(size)]


def merge_reports(reports):
    """Combine per-rank metric reports into a single summary."""
    combined = {}
    # sum all counters and total_time_s
    for key in reports[0].keys():
        if key != 'avg_time_s':
            combined[key] = sum(r.get(key, 0) for r in reports)
    # recompute average time per URL
    total_urls = combined.get('urls_total', 0)
    total_time = combined.get('total_time_s', 0.0)
    combined['avg_time_s'] = (total_time / total_urls) if total_urls else 0.0
    return combined


def main():
    logger = setup(verbose=False)
    metrics = Metrics()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # Define job list on rank 0
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

    # Distribute jobs
    my_jobs = comm.scatter(jobs, root=0)

    local_data = []
    t0 = MPI.Wtime()

    for url in my_jobs:
        metrics.incr('urls_total')
        with metrics.time_block():
            try:
                scraped = scrape_trending(url)
                metrics.incr('urls_success')
                for pos, repo in scraped:
                    local_data.append({'url': url, 'position': pos, 'repo': repo})
                logger.info(f"[rank {rank}] scraped {url} ({len(scraped)} items)")
            except Exception as e:
                metrics.incr('urls_failed')
                logger.warning(f"[rank {rank}] error {url}: {e}")

    # Gather data and metrics
    all_data = comm.gather(local_data, root=0)
    all_reports = comm.gather(metrics.report(), root=0)

    if rank == 0:
        # Flatten data
        flat = [item for sub in all_data for item in sub]
        # Write combined CSV
        with open('data/output/trending_parallel.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'position', 'repo'])
            writer.writeheader()
            writer.writerows(flat)
        logger.info(f"Wrote {len(flat)} rows to data/output/trending_parallel.csv")

        # Compute and save combined metrics
        combined = merge_reports(all_reports)
        logger.info(f"Parallel run duration (MPI.Wtime): {MPI.Wtime() - t0:.2f}s")
        logger.info(f"Metrics: {combined}")
        # Save to JSON
        metrics.save('metrics/parallel_metrics.json')


if __name__ == '__main__':
    main()
