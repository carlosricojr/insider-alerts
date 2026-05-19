# CodeRabbit PR Review Loop (Mandatory)

Use this workflow for any task that prepares, updates, or merges a pull request.

## Policy

1. Do not merge while CodeRabbit status is `PENDING`.
2. Do not merge while actionable CodeRabbit comments remain unresolved.
3. Apply fixes, push, and wait for CodeRabbit re-review until there is nothing left to address.
4. Run quality gates after fixes (`tsc`, `lint`, `build`, and relevant tests).
5. Use an autonomous check loop; do not ask the user for permission between routine re-checks.
6. Run GitHub/CodeRabbit polling commands with network-enabled execution (outside sandbox when sandbox networking is restricted).
7. Treat GitHub API/network failures as retryable; never treat transport errors as review completion.

## Required Loop

1. Open or update PR.
2. Wait for CodeRabbit review to complete.
3. Collect all CodeRabbit comments/review findings.
4. Triage each finding:
   - `must-fix`: correctness, regression risk, security, data integrity, scope contract violations.
   - `optional`: style/preferences with no correctness impact.
5. Implement all `must-fix` items.
6. Re-run quality gates and targeted tests.
7. Push changes.
8. Repeat from step 2 until no actionable findings remain.
9. Merge only after checks pass and CodeRabbit has no unresolved required feedback.

## Commands

- Check PR status and reviews:
  - `gh pr view <number> --json statusCheckRollup,reviews,comments,reviewRequests`
- Inspect comments:
  - `gh pr view <number> --comments`
- Poll checks in a loop (preferred):
  - `gh pr checks <number> --watch --interval 30`
  - If sandbox networking is restricted, run this outside the sandbox (escalated network call).
- Poll checks in an explicit bounded loop (fallback):
  - ```bash
    PR=<number>
    for i in $(seq 1 30); do
      echo "[${i}/30] checking CodeRabbit status"
      out=$(gh pr checks "$PR" 2>&1)
      code=$?
      if [ $code -ne 0 ]; then
        echo "GitHub API/network error. Retrying..."
        sleep 30
        continue
      fi
      if echo "$out" | rg -i "coderabbit" | rg -iv "pending|in_progress|queued"; then
        echo "CodeRabbit finished. Pulling review comments."
        gh pr view "$PR" --comments
        break
      fi
      sleep 30
    done
    ```
- Merge after clean review loop:
  - Before merging, ensure the PR title/squash commit title is Conventional Commit-compatible for Release Please.
  - `gh pr merge <number> --merge --delete-branch`

## Notes

- If a finding is intentionally not applied, document rationale clearly in PR comments.
- Keep fixes scoped; avoid unrelated refactors during review-response commits.
- The agent should stay in this loop until completion or timeout; if timeout is hit, report status and keep the PR unmerged.
- For Codex agents: when `gh` commands fail due sandbox network restrictions, rerun immediately with escalated permissions (outside sandbox). Do not misclassify this as CodeRabbit completion.
- PR is done only when:
  - spec exists and scope remains unchanged from that spec
  - tests/quality gates pass (`tsc`, `lint`, `build`, and required test suites)
  - deterministic replay behavior is unchanged, or intentionally changed with rationale documented
  - risk and rollback path are documented
  - decision note is updated
- Complete ONE task per iteration in the automated loop:
  - find the first unfinished task in the active sprint/spec checklist
  - implement only what is missing for that one task
  - run validation commands for that scope
  - mark task progress in docs/checklist
  - commit and push
  - return to CodeRabbit review
