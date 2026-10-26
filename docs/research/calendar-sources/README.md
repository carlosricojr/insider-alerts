# Reviewed calendar baseline

The archived PDF is the NYSE/ICE announcement published December 23, 2025:

https://ir.theice.com/press/news-details/2025/NYSE-Group-Announces-2026-2027-and-2028-Holiday-and-Early-Closings-Calendar/

PDF URL:
https://s2.q4cdn.com/154085107/files/doc_news/NYSE-Group-Announces-2026-2027-and-2028-Holiday-and-Early-Closings-Calendar-2025.pdf

SHA-256: `76775c27848d19dc15d7cac8927f6e85f63605aa715fe5401ed486aa722e1d23`.
The PDF supplies the holidays and early closes for 2026–2028. Regular core
equity hours (09:30–16:00 America/New_York) are documented separately at
https://www.nyse.com/markets/hours-calendars. The module uses core equity sessions,
not options, bonds, extended hours, or an instrument-specific trading guarantee.
Facts were transcribed manually and tested independently against the announcement.
The regular-hours webpage is not archived in this release: its hours are a
reviewed implementation constant, not a separately content-addressed source
snapshot. This provenance limitation must be resolved before commissioning.

## Isolated operation

The module `insider_alerts.research.calendar_v2` has no existing consumers. It
does not change live execution, repair OPP-E07-V1, or authorize enrollment.
Create a **new** journal only after reviewing the source and implementation:

```powershell
uv run python -m insider_alerts.research.calendar_v2 --journal <new-path.sqlite> --create-from-pdf docs/research/calendar-sources/nyse-holidays-2026-2028.pdf --review-reference <review-record>
uv run python -m insider_alerts.research.calendar_v2 --journal <existing-path.sqlite>
```

The creation command records the actual current observation time; publication
date does not imply historical possession. The API accepts observation times for
deterministic tests and trusted import workflows; callers must not backdate evidence.
Writes reject observations more than five seconds ahead of the local UTC clock.
`review_reference` is custody metadata, not proof of authorization. Source URL
validation does not authenticate the supplied bytes; a reviewer must do that.
The annual baseline always reports `BASELINE_ONLY`, never trial readiness.

## Corrections and limits

`CalendarJournal.append_notice` accepts reviewed explicit closures or unknown
dates with archived source bytes and an expected parent digest. It creates a new
version transactionally; previous versions and bindings remain unchanged.
The version hash covers observation time as well as facts and provenance. An
exact retry returns the committed version and its original observation time.
Retry recognition is limited to the current head; after another notice commits,
the old request fails with a parent conflict and must be reconciled by reading
history, not resubmitted with a new parent.
Use `as_of` for what was known at a cutoff and retain the binding digest and
version digest with future evidence. `binding_conflicts` reports subsequent
contradictions; consumers must invalidate affected evidence, never silently
repair its horizon. Unsupported years and unknown dates fail horizon binding.
Conflict checks require the original validated version, verify the binding
against it, and require the comparison version to descend from it. Only use
version objects returned by the validated journal reader, not hand-built objects.

This release cannot reopen a date or alter hours through notices. Such corrections
require a reviewed extension. Append-only triggers and hashes detect ordinary
corruption; they are not protection against a privileged actor replacing the
whole journal. Preserve external review records and future evidence bindings.
Hard-link publication requires a filesystem supporting same-volume hard links;
unsupported filesystems fail without publishing a journal.
Malformed records raise `CalendarError`; filesystem and SQLite operational errors
remain distinct retry/recovery boundaries. A crash leaving a hot journal can
require read-write SQLite recovery before read-only access succeeds. Never edit
baseline constants for existing journals; a new definition requires a new format
and a reviewed migration that preserves old evidence.

`missing_bar_dates` checks presence only. It does not certify finality, price
quality, receipt time, point-in-time features, or readiness. Before any new trial,
separately commission acquisition, extraordinary-closure detection, freshness,
version-conflict handling, and complete decision-time provenance. No journal or
scheduled task is installed by this release.
