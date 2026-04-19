# Cohort image fetch — summary

*Generated (UTC): `2026-04-19T11:21:23Z`*

## Sources

- **Log:** `/Users/markphillips/Downloads/MSC Project/data/processed/images/cohort_image_fetch.log`
- **Plan:** `/Users/markphillips/Downloads/MSC Project/data/processed/cohorts/multimodal_plan_n50000_seed42.tsv`

## Time range (log timestamps)

- **First:** 2026-04-19T10:26:42+00:00
- **Last:** 2026-04-19T10:26:44+00:00

## Totals

| Metric | Count |
|--------|------:|
| `ok` rows | 10 |
| `fail` rows | 4 |
| **Total** | 14 |
| Unique `sample_id` with ok | 10 |
| Unique `sample_id` with fail | 4 |
| Success rate (ok / all attempts) | 71.43% |

## Key facts (summary)

- **Multimodal volume:** the run logged **10** successful image saves (`ok`) from **14** attempts (**71.43%** overall success rate).
- **Corpus mix (successes):** **100.00%** of all `ok` rows are Fakeddit and **0.00%** are FakeNewsNet — consistent with a much larger eligible Fakeddit pool in the consolidated table.
- **Per-attempt difficulty:** Fakeddit image fetch succeeded **71.43%** of the time vs **0.00%** for FakeNewsNet (each corpus’s own attempts), reflecting Reddit/CDN behaviour and older news URLs.
- **Placeholder / stub traffic (Fakeddit):** **3** failures were classified as `reddit_placeholder_sha256` (known hash blocklist), i.e. many URLs still return *an* image but not a usable post image.
- **Primary vs reserve:** **4** successes joined to **`plan_role=primary`** and **6** to **`reserve`** — reaching 50k `ok` relied heavily on **reserve** backfill, not only primary slots.
- **Class balance (successes):** **`label_binary=1` (fake)** is **60.00%** of all **`ok`** rows and **`0` (real)** **40.00%** (denominator = total `ok` in the log; see table below). *(Convention: **0 = real**, **1 = fake**; confirm against your `fakenews.tsv` build.)*
- **Manual QC:** automated checks do not verify *semantic* image–text alignment; consider a **small stratified spot sample** (dataset × label × outcome) for the write-up.

## OK successes: real vs fake (`label_binary`)

Joined **`ok`** log lines to the cohort plan on **`sample_id`**. Convention: **`0` = real**, **`1` = fake** (same as consolidated `fakenews.tsv`; verify if you changed mapping).

| Class | `label_binary` | Count | % of all `ok` |
|-------|----------------|------:|--------------:|
| Real | `0` | 4 | **40.00%** |
| Fake | `1` | 6 | **60.00%** |
| **Total `ok`** | | 10 | **100%** |

*Reconciliation: **`ok`** with plan join (any `label_binary` field): **10**; `ok` **not** in plan file: **0**.*

| Dataset | `0` (real) | % within corpus `ok` | `1` (fake) | % within corpus `ok` | Total `ok` |
|---------|------------:|-------------------------:|-----------:|-------------------------:|-----------:|
| fakeddit | 4 | **40.00%** | 6 | **60.00%** | 10 |
| fakenewsnet | 0 | **0.00%** | 0 | **0.00%** | 0 |

## By dataset

| Dataset | `ok` | % of all `ok` | `fail` |
|---------|-----:|----------------:|-------:|
| fakeddit | 10 | **100.00%** | 4 |

The **% of all `ok`** column is the corpus share of every successful image save (sums to 100%).

## Vs cohort plan (join on `sample_id`)

- **Rows in plan file:** primary 50,000 · reserve 150,000
- **Nominal primary target *N* (for %):** 50,000
- **`ok` on primary rows:** 4 (0.01% of *N*)
- **`ok` on reserve rows:** 6
- **`fail` on primary rows:** 1
- **`fail` on reserve rows:** 3
- **`ok` not found in plan:** 0
- **`fail` not found in plan:** 0

## Failure `detail` field

The cohort fetcher (`pipeline/06_cohort_fetch_images.py`, `_download_one`) writes either a **fixed string** after local checks (size, SHA blocklist, PIL verify) or **`type(e).__name__`** from the HTTP stack (typically `requests` / `urllib3`). The log does not store full stack traces or HTTP status lines for most errors.

### Observed in this log (top 20)

| Count | Code / `detail` | Meaning |
|------:|------------------|---------|
| 3 | `reddit_placeholder_sha256` | Downloaded bytes matched a SHA-256 on the project blocklist (typically Reddit/i.redd.it placeholder or CDN “missing image” payloads). The URL returned *something*, but it is treated as unusable for training. |
| 1 | `HTTPError` | The `requests` library raised after `raise_for_status()`—typically HTTP 4xx/5xx (removed post, paywall, forbidden, server error). The log stores the exception class name only, not the status line. |

### Reference: codes used by the fetcher

| Code | Meaning |
|------|---------|
| `ChunkedEncodingError` | Transfer-Encoding/chunked stream ended inconsistently (server or intermediary closed the connection early). |
| `ConnectTimeout` | Timed out while trying to establish a TCP connection to the server. |
| `ConnectionError` | Network-level failure connecting to the host (DNS, refused connection, reset, proxy issues, offline client, or remote server not accepting the connection). |
| `ContentDecodingError` | Response declared a compression encoding but bytes could not be decoded. |
| `HTTPError` | The `requests` library raised after `raise_for_status()`—typically HTTP 4xx/5xx (removed post, paywall, forbidden, server error). The log stores the exception class name only, not the status line. |
| `InvalidSchema` | URL scheme is not supported for this request configuration. |
| `InvalidURL` | URL string was malformed or not usable as an HTTP URL. |
| `MissingSchema` | URL lacked `http://` or `https://` (or similar), so `requests` could not fetch it. |
| `ReadTimeout` | Connection opened, but the server did not send a complete response within the read timeout. |
| `SSLError` | TLS/SSL handshake or certificate validation failed between client and server. |
| `Timeout` | HTTP request exceeded the configured timeout (connect or read phase; see the `requests` `Timeout` hierarchy). |
| `TooManyRedirects` | Redirect loop or excessive redirects when following the image URL (`allow_redirects=True` in the fetcher). |
| `pil_verify_failed` | Bytes did not pass a strict PIL open+verify check—corrupt file, wrong content type (HTML/JSON masquerading as an image), or unsupported/broken image data. |
| `reddit_placeholder_sha256` | Downloaded bytes matched a SHA-256 on the project blocklist (typically Reddit/i.redd.it placeholder or CDN “missing image” payloads). The URL returned *something*, but it is treated as unusable for training. |
| `too_small` | HTTP response body was smaller than 512 bytes (`06_cohort_fetch_images.py`). Often an error page, empty payload, or truncated response rather than a real image. |
| `(empty)` | No text was stored in the log’s `detail` column (unexpected for normal runs). |

*Other `detail` values that look like Python class names (`ConnectError`, `OSError`, …) are passed through from the same generic `except` path; check the Requests/urllib3 documentation for that type.*
