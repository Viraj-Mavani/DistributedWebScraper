# scraper/urlgen.py

import urllib.parse
from typing import List

def generate_trending_urls(
    languages: List[str] = None,
    periods:   List[str] = None,
    spoken_languages:   List[str] = None
) -> List[str]:
    """
    Build GitHub Trending URLs for the given languages and time windows.

    - languages: e.g. ['Python','JavaScript','Java','C','Go','c%23'] (case-insensitive)
    - periods:   e.g. ['daily','weekly','monthly']
    - spoken_languages:   e.g. ['','en']

    Always includes the global (all-languages) URL for each period.
    """
    if periods   is None: periods   = ['daily','weekly','monthly']
    if languages is None: languages = []  # no language filters
    if spoken_languages is None: spoken_languages = ['']  # no spoken_languages filters

    urls: List[str] = []
    for period in periods:
        for spoken_lang in spoken_languages:
            # global Trending
            spoken_lang_param = f'&spoken_language_code={spoken_lang}' if spoken_lang else ''
            urls.append(f'https://github.com/trending?since={period}{spoken_lang_param}')

            # per-language Trending
            for lang in languages:
                # quote e.g. "C#" → "c%23", "C++" → "c%2B%2B"
                enc = urllib.parse.quote_plus(lang.lower())
                urls.append(f'https://github.com/trending/{enc}?since={period}{spoken_lang_param}')
    return urls

