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
- The [SEC electronic-filing rule](https://www.sec.gov/files/rules/final/33-8230.htm) mandated
  electronic Forms 3, 4, and 5 only from June 30, 2003. Earlier EDGAR records can omit paper filings
  and cannot establish lifetime history. The registered observation boundary is therefore the bulk
  archive's first full calendar year, `2006-01-01`, rather than a false claim of prehistory
  completeness.

Each manifest and ZIP response is stored by SHA-256 before parsing. Retrieval rows preserve the
request and final URLs, retrieval time, status, media type, ETag, Last-Modified value, and an HTTP
content digest when supplied. Normalized rows retain their source archive digest. Archive
snapshots bind exactly one release to every quarter and fail if any quarter from 2006 Q1 through
the requested boundary is absent.

All source, retrieval, normalized, and snapshot tables reject updates and deletes. A refreshed SEC
archive becomes another release and another snapshot; it never overwrites the earlier view.

## Classifier behavior

At the January 1 cutoff for year `Y`, only filings dated before the cutoff and transactions dated
in prior years are visible. The classifier replays annual states from the fixed boundary and:

1. keeps only non-derivative open-market `P`/`S` rows with consistent acquired/disposed codes;
2. requires an exact reporting-owner CIK and rejects transaction attribution from multi-owner
   filings;
3. treats an amendment as a replacement only when it maps uniquely to an original filing visible
   at that cutoff, and rejects unresolved or same-day conflicting amendment order;
4. starts `unpartitionable`, then assigns the first complete three-year window to `routine` when a
   common calendar month exists or `opportunistic` when it does not;
5. preserves opportunistic status through later incomplete or disjoint windows, while checking each
   later complete window for a transition to routine;
6. keeps routine absorbing; and
7. otherwise returns `unpartitionable` with a typed reason.

Every result carries the observation start date, immutable source snapshot SHA-256, and a digest of
the exact bounded history input. Pre-2006 left-censoring remains explicit but is a measurement
limitation, matching the paper's finite-dataset implementation, rather than a reason to exclude the
entire opportunistic cohort. A stale snapshot, any archive gap, unresolved amendment, invalid
transaction, or ambiguous owner mapping still fails closed to `unpartitionable`.

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
