import json

def chunk_urls(urls, size):
    """
    Round-robin split: returns a list of `size` sublists,
    where sublist i contains urls[i::size].
    """
    return [urls[i::size] for i in range(size)]


def merge_reports(reports):
    """
    Combine a list of metrics-report dicts into a single summary.

    Each report should include counters like 'urls_total', 'urls_success', etc.,
    plus 'total_time_s'. This function sums counters and recomputes 'avg_time_s'.
    """
    combined = {}
    # sum all numeric fields except avg_time_s
    for rep in reports:
        for k, v in rep.items():
            if k == 'avg_time_s':
                continue
            combined[k] = combined.get(k, 0) + v
    # recompute average time per URL
    total_urls = combined.get('urls_total', 0)
    total_time = combined.get('total_time_s', 0.0)
    combined['avg_time_s'] = (total_time / total_urls) if total_urls else 0.0
    return combined


def save_report(report, path):
    """
    Save a combined report dict as formatted JSON to the given file path.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2)
