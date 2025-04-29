from mpi4py import MPI
import requests
from bs4 import BeautifulSoup
import csv

def scrape_trending(url):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    return [
        (idx, card.select_one('h2 a')
             .get_text(strip=True)
             .replace(' / ', '/'))
        for idx, card in enumerate(soup.select('article.Box-row'), start=1)
    ]

def chunk_urls(urls, size):
    return [urls[i::size] for i in range(size)]

def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

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

    # start global timer
    t0 = MPI.Wtime()

    my_urls = comm.scatter(jobs, root=0)
    local_data = []
    for url in my_urls:
        for pos, repo in scrape_trending(url):
            local_data.append({'url': url, 'position': pos, 'repo': repo})
        print(f"[rank {rank}] done {url} ({len(local_data)} rows)")

    all_data = comm.gather(local_data, root=0)

    if rank == 0:
        flat = [item for sub in all_data for item in sub]
        with open('trending_parallel_demo.csv', 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
            writer.writeheader()
            writer.writerows(flat)

        total_time = MPI.Wtime() - t0
        print(f"Parallel scrape: {len(flat)} rows in {total_time:.2f} s")

if __name__ == '__main__':
    main()

# run with "mpiexec -n 4 python parallel_demo.py"
