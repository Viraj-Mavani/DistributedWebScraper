import time
import requests
from bs4 import BeautifulSoup
import csv

def scrape_trending(url):
    resp = requests.get(url)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    out = []
    for idx, card in enumerate(soup.select('article.Box-row'), start=1):
        name = card.select_one('h2 a').get_text(strip=True).replace(' / ', '/')
        out.append((idx, name))
    return out

def main():
    urls = [
        'https://github.com/trending?since=daily',
        'https://github.com/trending?since=weekly',
        'https://github.com/trending/python?since=daily',
        'https://github.com/trending/python?since=weekly',
    ]

    start = time.time()
    all_data = []
    for url in urls:
        print(f"Fetching {url}…")
        for pos, repo in scrape_trending(url):
            all_data.append({'url': url, 'position': pos, 'repo': repo})
    duration = time.time() - start

    # write CSV
    with open('trending_serial_demo.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['url','position','repo'])
        writer.writeheader()
        writer.writerows(all_data)

    print(f"Serial scrape: {len(all_data)} rows in {duration:.2f} s")

if __name__ == '__main__':
    main()

# run with "python serial_demo.py"