import json
import os
import hashlib

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

def _compute_hash(urls):
    """Stable hash of the sorted URL list."""
    h = hashlib.sha256()
    h.update("||".join(sorted(urls)).encode("utf-8"))
    return h.hexdigest()

def load_checkpoint(all_urls, meta_path):
    """
    Load or initialize checkpoint metadata.
    Returns (meta, pending_urls).
    meta has keys: url_list_hash, all_urls, completed (list).
    """
    url_hash = _compute_hash(all_urls)
    # load existing meta or start fresh
    if os.path.exists(meta_path):
        with open(meta_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
    else:
        meta = {"url_list_hash": None, "all_urls": [], "completed": []}

    # decide reset if URLs changed or fully done
    if meta.get("url_list_hash") != url_hash or set(meta.get("completed", [])) == set(all_urls):
        # reset
        meta = {
            "url_list_hash": url_hash,
            "all_urls": list(all_urls),
            "completed": []
        }

    # pending = those in all_urls not yet in completed
    done_set = set(meta["completed"])
    pending = [u for u in all_urls if u not in done_set]
    return meta, pending

def save_checkpoint(meta, meta_path):
    """Overwrite the checkpoint JSON file with updated meta."""
    os.makedirs(os.path.dirname(meta_path), exist_ok=True)
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
