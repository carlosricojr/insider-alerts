# CodeRabbit PR Review Loop (Mandatory)

Canonical workflow for any task that prepares, updates, or merges a pull request.
The goal is to ship correct production code with the fewest round-trips by reviewing
**left** (locally, before the push) and using the PR bot as a final safety net rather
than the first line of defense.

## The model: two-stage, shift-left review

1. **Stage 1 — local, pre-push (does the heavy lifting).** Run the quality gates, do a
   deep adversarial self-review, then run the CodeRabbit **CLI** on the exact diff the PR
   will contain. Fix everything real before anything leaves the machine.
2. **Stage 2 — the PR bot + CI (the straggler net).** Push, open the PR, and let the
   CodeRabbit PR bot and CI catch what slipped through. Triage, fix, merge clean.

> The CLI and the PR bot read the **same** `.coderabbit.yaml`, so they run at the same
> profile (this repo's PR bot is `CHILL` by design). The local pass earns its keep by
> moving that review *before* the push so PRs land clean; the deep self-review in Stage 1
> is what supplies the assertive depth a `CHILL` profile intentionally omits.

**Non-negotiable principle — verify before you act.** CodeRabbit (CLI *and* bot) produce
false positives and out-of-date claims. Never apply a finding blind: re-read the cited
code first, confirm the issue is real against current source, and only then fix it. If a
finding is wrong, say why and move on (in Stage 2, reply in-thread with the evidence and
resolve the thread). A green review with one refuted false positive is a pass; a fix
applied to satisfy a false claim is a regression.

## Stage 0 — Orient & scope (before writing code)

- Read `AGENTS.md` / `CLAUDE.md` / `.claude` rules and the code, tests, and nearby docs
  for the area you're changing. Follow them exactly. Make the smallest coherent change.
- Do **not** bundle unrelated working-tree changes into the PR. If the tree has WIP that
  isn't part of this unit, summarize it and confirm grouping before committing.
- Branch from the repo's integration/default branch
  (`gh repo view --json defaultBranchRef -q .defaultBranchRef.name`); never commit straight
  to it. Some repos promote `staging` → `main`: target the integration branch for
  feature/fix work and reserve `main` for release PRs.
- Conventional Commit titles (the merge/squash title drives Release Please / changesets
  where present). No AI/assistant attribution in commits or PRs. Do not hand-bump version
  files or `CHANGELOG`.

## Stage 1 — Pre-push local review (catch it before the push)

1. Run the repo's own gates and fix until green: typecheck, lint, tests, and `build` when
   it's quick (`tsc --noEmit`, the repo's `lint`/`test`/`build` scripts).
2. **Deep self-review.** Re-read the full diff as an adversarial reviewer across
   correctness, security, repo-constraint/regression, a11y/perf, and copy. Verify each
   concern against source; fix the real ones. For larger or higher-risk diffs, fan this out
   (multiple independent review passes/agents) and adversarially verify each finding.
3. **CodeRabbit CLI review** on the diff the PR will contain:
   - Confirm readiness: `coderabbit doctor` (authenticate once with
     `coderabbit auth login --agent` if needed).
   - `coderabbit review --base <integration-branch> --agent`
   - Verify every finding against current source. Fix the real ones; for false positives,
     note the reason and skip. Re-run until no actionable findings remain.
   - Optional for trivial/docs-only changes (it spends review budget); always run it for
     behavior changes.
4. Re-run the gates after fixes. Only push when Stage 1 is clean.

## Stage 2 — Open the PR & clear the bot net

1. Push the branch and open a PR to the integration branch. Body states **what** changed,
   **why**, and **how you verified** (gates + that a local CodeRabbit pass ran).
2. Wait for CI **and** the CodeRabbit PR bot to finish (see Commands). Treat
   network/API errors as retryable — never mistake a transport error for review completion.
3. Triage every finding: verify against source, then fix real ones with follow-up commits
   (a push re-triggers review). For a false positive, reply in-thread with the evidence and
   resolve the thread. Document any intentional divergence in-thread.
4. Repeat until CI is green and there are **zero unresolved actionable** review threads.
5. Stay autonomous through routine re-checks; don't ask for permission between polls.

## Merge gates

Merge only when **all** hold:
- CI is green and the CodeRabbit bot is not `PENDING`.
- No unresolved actionable review threads (false positives refuted + resolved).
- Scope is unchanged from what the task/spec implied; risk and rollback path are clear, and
  deterministic-replay behavior is unchanged (or intentionally changed with rationale noted).
- The PR/squash title is a valid Conventional Commit.

Then merge with the repo's convention and delete the branch. **If branch protection requires
a human approval that the agent cannot satisfy, stop and hand the PR off — do not merge.**

## Commands

- Status + reviews: `gh pr view <number> --json statusCheckRollup,reviews,comments,reviewRequests`
- Comments: `gh pr view <number> --comments`
- Poll checks (preferred): `gh pr checks <number> --watch --interval 30`
- Poll checks (scriptable, bucket-based completion — robust to formatting changes):
  ```bash
  PR=<number>
  for i in $(seq 1 40); do
    out=$(gh pr checks "$PR" --json name,bucket 2>&1) || { echo "[$i] API/network error; retrying"; sleep 30; continue; }
    done_cr=$(echo "$out" | jq -r '.[] | select((.name|ascii_downcase)=="coderabbit") | select((.bucket|ascii_downcase)|IN("pass","fail","skipping","cancel","cancelled")) | .bucket')
    if [ -n "$done_cr" ]; then echo "CodeRabbit finished ($done_cr)"; gh pr view "$PR" --comments; break; fi
    echo "[$i/40] CodeRabbit still pending"; sleep 30
  done
  ```
- Resolve a refuted thread (after replying with evidence): use the GraphQL
  `resolveReviewThread` mutation on the thread id from
  `gh api graphql` (`pullRequest.reviewThreads`).
- Merge after a clean loop: `gh pr merge <number> --merge --delete-branch`
  (use the repo's merge style; ensure the title is Conventional-Commit-compatible first).

## UI Polish Checks

Apply when a PR touches UI, layout, motion, or shared primitives:
- No broad `transition-all` on app components or primitives; transition explicit properties.
- Motion respects `prefers-reduced-motion` — overlay/menu entrances, loaders, counters, and
  decorative background motion included.
- Hover-revealed controls also work with keyboard focus and touch.
- Fixed/sticky offsets are tied to layout variables or live measurement.
- First-viewport content and LCP candidates render in SSR HTML — not gated on client-only
  viewport/intersection checks or hydration-only Motion `initial` states.
- Deferred callbacks, `setTimeout`, smooth scroll, and RAF work are canceled or re-check
  the current interaction state.
- App surfaces use semantic tokens; add a missing token before using a class for it.
- Decorative background/canvas/map systems don't measure transformed or animating elements
  for obstacle/layout logic.

## Notes

- Keep review-response commits scoped — no unrelated refactors while clearing findings.
- Each `coderabbit review` is a full AI review and spends budget; run it once the diff has
  settled, not on every keystroke.
- Complete one logical unit per loop iteration: implement what that unit needs, validate it,
  commit, push, and return to the review loop.
- If a timeout is hit before the loop completes, report status and leave the PR unmerged.
