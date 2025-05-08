# scraper/metrics.py

import time
import json
from contextlib import contextmanager

class Metrics:
    """
    Track scraping metrics such as counts of successes/failures, parse errors,
    retries, and timings for each URL fetch+parse.
    """
    def __init__(self):
        # Initialize counters
        self.counters = {
            'urls_total':   0,
            'urls_success': 0,
            'urls_failed':  0,
            'parse_errors': 0,
            'retries':      0,
            'duplicates_removed': 0,
        }
        # List of per-URL durations
        self.timings = []

    def incr(self, key: str, amount: int = 1):
        """
        Increment a counter by amount.
        """
        self.counters[key] = self.counters.get(key, 0) + amount

    @contextmanager
    def time_block(self):
        """
        Context manager to time a block and record its duration.

        Usage:
            with metrics.time_block():
                # code to measure
        """
        start = time.time()
        try:
            yield
        finally:
            duration = time.time() - start
            self.timings.append(duration)

    def report(self) -> dict:
        """
        Return a summary of counters and timing metrics.
        """
        total_time = sum(self.timings)
        avg_time = total_time / len(self.timings) if self.timings else 0.0
        return {
            **self.counters,
            'total_time_s': total_time,
            'avg_time_s':   avg_time
        }

    def save(self, path: str):
        """
        Save the metrics report as JSON to the given file path.
        """
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.report(), f, indent=2)
