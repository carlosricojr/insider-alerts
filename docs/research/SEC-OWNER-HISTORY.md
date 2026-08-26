# SEC owner-history source and classifier contract

This component reconstructs reporting-owner histories for the draft `OPP-E07-V1` trial. It is
research-only and imports no broker or order code. Nothing here activates the trial or changes the
live canary.

## Authoritative inputs

- The [SEC Insider Transactions Data Sets](https://www.sec.gov/data-research/sec-markets-data/insider-transactions-data-sets)
  are the source for January 2006 onward. The synchronizer discovers every ZIP from the links on
  that page because SEC moved the download namespace in 2026.
- The [SEC dataset documentation](https://www.sec.gov/files/insider_transactions_readme.pdf)
  defines the submission, reporting-owner, and non-derivative transaction keys and states that
  amendments remain present as filed.
- The [SEC submissions API](https://www.sec.gov/search-filings/edgar-application-programming-interfaces)
  and EDGAR filing documents are the future source for owner-specific pre-2006 investigation.
  Their existence does not, by itself, prove that pre-electronic or paper history is complete.

Each manifest and ZIP response is stored by SHA-256 before parsing. Retrieval rows preserve the
request and final URLs, retrieval time, status, media type, ETag, Last-Modified value, and an HTTP
content digest when supplied. Normalized rows retain their source archive digest. Archive
snapshots bind exactly one release to every quarter and fail if any quarter from 2006 Q1 through
the requested boundary is absent.

All source, retrieval, normalized, and snapshot tables reject updates and deletes. A refreshed SEC
archive becomes another release and another snapshot; it never overwrites the earlier view.

## Classifier behavior

At the January 1 cutoff for year `Y`, only filings dated before the cutoff and transactions dated
in prior years are visible. The classifier:

1. keeps only non-derivative open-market `P`/`S` rows with consistent acquired/disposed codes;
2. requires an exact reporting-owner CIK and rejects transaction attribution from multi-owner
   filings;
3. treats an amendment as a replacement only when it maps uniquely to an original filing visible
   at that cutoff, and rejects unresolved or same-day conflicting amendment order;
4. returns `routine` after a positive same-calendar-month pattern in three consecutive years and
   keeps that state absorbing;
5. returns `opportunistic` only when the preceding three years all have qualifying trades with no
   common month **and** the caller has separately established complete prehistory; and
6. otherwise returns `unpartitionable` with a typed reason.

The asymmetry is intentional: incomplete early history can positively prove that an owner became
routine, but it can never prove that the owner was opportunistic. The bulk archive's 2006 start is
therefore always left-censored unless separate authoritative evidence clears it. The synchronizer
does not set `prehistory_complete`; that state is rejected unless it also carries the SHA-256 of a
reviewed coverage artifact. Producing that artifact requires the owner-specific work and evidence
review in the remaining M3 slice.

## Operation

Run the order-incapable module from a reviewed deployment checkout:

```powershell
.venv\Scripts\python.exe -m insider_alerts.research.history_worker
```

Defaults are `data/research/sec_history.db` and `data/research/sec-history-raw`. An interrupted run
is resumable: already verified releases are reused. `--refresh` deliberately re-fetches all
published releases so SEC corrections produce a new immutable snapshot. `--through-year` and
`--through-quarter` must be supplied together and exist primarily for bounded validation.

Use `pythonw.exe` with hidden/no-window task settings for unattended operation. This milestone does
not install a recurring task; owner-history refresh is not latency-sensitive and activation remains
blocked until the remainder of M3 and the frozen inference executable are reviewed and deployed.
