# BPRE broker rejection evidence

PR69 recovered the missing prior-day exit pointer and passed all gates, but live
IBKR rejected the intended replacement sell while preserving existing stop/target.
A what-if simulation of the same SELL16/OCA reported broker code201, equity with
loan value163.61 below initial margin278.14. This is diagnostic simulation evidence,
not proof of the actual rejected order reason or authority to add capital.

Retain bounded order-specific broker errorEvent codes/messages for MOC and market
exit rejection, sanitize account identifiers, and avoid order/risk setting changes.
Existing trade.log had empty messages, so it could not establish the reason.
Reuse reviewed-design logic from the earlier isolated MOC diagnostics worktree;
that unmerged work remains preserved. The previous read-only design challenge
required exact broker evidence before a routing change; perform adversarial review,
full gates and canonical exact-head PR review. Deploy reviewed main, keeping the
interrupted commissioning collector disabled/evidence intact. Verify fresh actual
rejection details before selecting any further order-routing correction.

Handoff: diagnostic implementation in isolated branch, no new live behavior.
