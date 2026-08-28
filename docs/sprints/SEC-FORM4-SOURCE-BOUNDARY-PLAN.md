# SEC Form 4 Source-Boundary Remediation

Status: implementation and local review complete; deployment verification pending

## Outcome

Prevent non-Form-4 SEC current-filings entries and unrelated filing XML documents from consuming
the bounded enrichment and review queues, while preserving all historical records unchanged.

## Verified production symptom and cause

- The live database contains 431,330 filings. Of 471 rows missing a selected XML document, the
  recent rows are SEC RSS entries such as `485BPOS` and `497` that were stored as Form 4.
- RSS parsing searched title, description, GUID, category, and URL with `\b4(?:/A)?\b`. This
  treated text such as `Size: 4 MB` as the filing form.
- Filing-index parsing fell back to any non-XSL XML link. For non-Form-4 filings this selected
  taxonomy, presentation, label, or filing-fee XML and caused repeated parser failures.
- Each worker cycle is bounded to 100 recent rows, so those false rows can starve genuine Form 4
  work even while the cycle heartbeat remains successful.

## Constraints

- Do not delete, rewrite, or backfill historical filing or evidence rows.
- Do not change strategy selection, order behavior, account type, or capital limits.
- Keep SEC failures isolated and observable; fail closed on ambiguous filing identity.
- Keep all production workers invisible and use only the existing hidden scheduled-task path.
- Claude design critique was attempted first but unavailable because its usage limit was reached.
  A read-only Codex design challenge was attempted as the documented substitute; it stalled during
  database inspection and was terminated without modifying files or runtime state.

## Implementation

1. Accept an RSS/Atom entry only when its title prefix or exact category identifies `4` or `4/A`;
   persist the validated feed form type and aggregate rejected/invalid source-item diagnostics.
2. In a recognized SEC `Document Format Files` table, accept only an XML link from a row whose
   `Type` cell is exactly `4` or `4/A`; prefer the raw link over an XSL-transformed link.
3. When no recognized document table exists, retain only the narrow form-like filename fallback
   required by older fixtures and sparse pages. Never fall back to arbitrary XML, malformed URLs,
   non-HTTPS URLs, or off-domain URLs.
4. Apply the same source-provenance predicate to both the missing-XML queue and review queue so
   immutable legacy false positives cannot consume their limits.
5. Prove the boundary with focused regressions, the complete quality gate, adversarial review,
   CodeRabbit exact-head review, and a post-deploy live cycle.

## Acceptance checks

- `485BPOS`, `497`, and other prefix forms containing a standalone digit 4 are rejected.
- Exact `4` and `4/A` RSS and Atom entries remain accepted.
- Source items seen, source-boundary rejections, and invalid items are observable per poll cycle.
- A generic `primary_doc.xml` in a document row typed `4` is selected.
- A taxonomy-only or non-Form-4 document table yields no XML URL.
- A legacy false RSS row cannot displace a valid row at a queue limit of one.
- Missing-title RSS rows and non-RSS rows remain eligible for backward compatibility.
- `uv run ruff check .`, `uv run mypy src`, and `uv run pytest` pass.
- After deployment, the live worker has a fresh successful cycle, no false-row saturation, a clean
  `main == origin/main` checkout, and no visible-terminal task regression.
