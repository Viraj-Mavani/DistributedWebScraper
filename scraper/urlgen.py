# scraper/urlgen.py

import urllib.parse
from typing import List


def generate_trending_urls(
        languages: List[str] = None,
        periods: List[str] = None,
        spoken_languages: List[str] = None
) -> List[str]:
    """
    Build GitHub Trending URLs for the given languages and time windows.

    - languages: e.g. ['Python','JavaScript','Java','C','Go','c%23'] (case-insensitive)
    - periods:   e.g. ['daily','weekly','monthly']
    - spoken_languages:   e.g. ['','en']
    """
    if periods is None: periods = ['daily']
    if languages is None: languages = []
    if spoken_languages is None: spoken_languages = ['']  # will produce &spoken_language_code=

    urls: List[str] = []

    for period in periods:
        for spoken in spoken_languages:
            sl = f"&spoken_language_code={spoken}"

            if not languages:
                # GLOBAL trending
                urls.append(f"https://github.com/trending?since={period}{sl}")
            else:
                # per-language only
                for lang in languages:
                    enc = urllib.parse.quote_plus(lang.lower())
                    urls.append(f"https://github.com/trending/{enc}?since={period}{sl}")

    return urls
