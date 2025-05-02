# scraper/urlgen.py

import urllib.parse
from typing import List

def generate_trending_urls(
    languages: List[str] = None,
    periods:   List[str] = None
) -> List[str]:
    """
    Build GitHub Trending URLs for the given languages and time windows.

    - languages: e.g. ['Python','JavaScript','C', 'c%23'] (case-insensitive)
    - periods:   e.g. ['daily','weekly','monthly']

    Always includes the global (all-languages) URL for each period.
    """
    if periods   is None: periods   = ['daily','weekly','monthly']
    if languages is None: languages = []  # no language filters

    urls: List[str] = []
    for period in periods:
        # global Trending
        urls.append(f'https://github.com/trending?since={period}')
        # per-language Trending
        for lang in languages:
            # quote e.g. "C#" → "c%23", "C++" → "c%2B%2B"
            enc = urllib.parse.quote_plus(lang.lower())
            urls.append(f'https://github.com/trending/{enc}?since={period}')
    return urls
