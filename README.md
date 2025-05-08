# Distributed Web Scraper using MPI

A Python-based distributed web scraper for GitHub Trending pages, leveraging MPI (via mpi4py) for parallelism and CloudScraper for handling Cloudflare/UAM challenges.

## Features
- **Serial and Parallel Execution**: Run in single-process mode (`serial_main.py`) or multi-process mode using MPI (`parallel_main.py`).
- **Checkpointing**: Save progress to resume scraping if interrupted.
- **Configurable**: All settings are controlled via `config.yaml`.
- **Metrics & Reporting**: Track URLs processed, successes, failures, retries, timing, and duplicate removals.
- **Post-scrape Deduplication & Sorting**: Remove duplicate entries and sort results by `source_url`.
- **Modular Structure**: Core scraping logic in `scraper/core.py`, URL generation in `scraper/urlgen.py`, scheduling in `scraper/scheduler.py`, and logging setup in `scraper/logger.py`.

## File Structure
```
.
├── config.yaml           # Configuration for scraper settings, paths, and filters
├── serial_main.py        # Entry point for serial scraping
├── parallel_main.py      # Entry point for MPI-based parallel scraping
├── scraper/
│   ├── core.py           # Scraping and parsing functions
│   ├── urlgen.py         # Generates GitHub Trending URL list
│   ├── scheduler.py      # Checkpoint load/save and report merge
│   ├── logger.py         # Logging configuration
│   └── metrics.py        # Metrics collection and reporting
├── data/
│   └── output/
│       ├── checkpoint.json
│       ├── trending_serial.csv
│       └── trending_parallel.csv
├── metrics/
│   ├── serial_metrics.json
│   └── parallel_metrics.json
└── README.md            # This file
```

## Configuration (`config.yaml`)
```yaml
logging:
  level: INFO

scraper:
  max_retries: 3
  timeout: 10              # HTTP timeout in seconds
  user_agent: >-
    Mozilla/5.0 (Windows NT 10.0; Win64; x64)
    AppleWebKit/537.36 (KHTML, like Gecko)
    Chrome/116.0.0.0 Safari/537.36
  accept_language: "en-US,en;q=0.9"
  referer: "https://github.com/"
  sleep_between_requests:
    min: 0.5
    max: 1.0

trending:
  languages: ["Python"]
  periods:   ["daily","weekly","monthly"]
  spoken_languages: ["", "en"]

paths:
  checkpoint:   "data/output/checkpoint.json"
  serial_csv:   "data/output/trending_serial.csv"
  parallel_csv: "data/output/trending_parallel.csv"
  metrics_dir:  "metrics"
```

## Usage

### Serial Execution
```bash
python serial_main.py
```
- Resumes from last checkpoint.
- Outputs CSV to `data/output/trending_serial.csv`.
- Saves metrics to `metrics/serial_metrics.json`.

### Parallel Execution
```bash
mpiexec -n <num_processes> python parallel_main.py
```
- Master process dispatches URLs to workers.
- Outputs CSV to `data/output/trending_parallel.csv`.
- Saves combined metrics to `metrics/parallel_metrics.json`.

## Post-scrape Processing
After scraping completes, duplicate rows are removed and the CSV is sorted by `source_url`. The number of duplicates removed is added to the metrics report under `duplicates_removed`.

## Dependencies
- Python 3.8+
- mpi4py
- cloudscraper
- requests
- beautifulsoup4
- pyyaml

Install via:
```bash
pip install mpi4py cloudscraper requests beautifulsoup4 pyyaml
```

## Improvements & Tuning
- **Add CLI**: Replace hardcoded filters with command-line arguments.
- **Rate Limiting & Backoff**: Smarter handling of GitHub rate limits.
- **Output Formats**: Support JSON, Parquet, or database ingestion.

