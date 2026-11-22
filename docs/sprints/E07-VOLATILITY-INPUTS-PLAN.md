# Qualification of archived E07 volatility inputs

Status: outcome-blind development. The archived acquisition run is preserved
in its original format and excluded from the new confirmatory cohort.

Read and verify the archived journal and the calendar version named by its
activation. For every first admitted case, require a single R1 response covering
the exact 21 prior exchange sessions, with source validation, an aware clock,
and a finalization and journal-record timestamp before 09:18 New York. Report
receipt coverage, recording timing and issuer-link coverage separately. The
coverage command emits no prices, forecasts or outcomes. The complete archive
chain is checked for integrity. Only R1 payloads enter qualification.

The coverage receipt binds the source journal watermark, frozen calendar digest
and filter version. It never creates a research decision, qualifies retrospective
issuer identity from a ticker match, or changes a broker subscription. Prospective
custody still needs the actual input digest and a durable decision read before
the relevant cutoff. For the historical replay, also verify outcome price
continuity and the relevant securities settlement calendar.

Verification uses synthetic custody cases for missing, late, invalid, duplicate,
and correctly qualified receipts, plus a read-only run against the archived
data. Pass the full repository gates and a settled-head review before merging.
