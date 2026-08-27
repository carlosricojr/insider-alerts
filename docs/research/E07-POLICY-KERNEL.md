# Pure E07/F00 policy kernel

Objective: make the already-frozen E07/F00 selection and daily-bar outcome rules one
side-effect-free implementation shared by the live-control canary and the future OPP-E07-V1 shadow
runner. This is necessary before activation because two copied implementations could silently
diverge after confirmatory enrollment begins.

A smaller change that leaves the outcome loop in `execution/canary.py` would not give the
order-incapable research runtime an independent import boundary. This change moves only pure policy:
rank, entry-session selection, eligibility, whole-share planning, and stop-first daily-bar outcome
evaluation. It does not add an outcome feed, trial persistence, activation, broker call, socket,
order path, feature, or threshold.

The kernel accepts typed values and returns typed values. It imports only shared backtest bar models
and standard-library modules. The canary remains the owner of broker interaction, live state,
capacity state, persistence, and orders. Existing canary tests are the regression gate; direct
kernel tests freeze incomplete-entry, gap-through-stop, same-bar collision, target, and tenth-session
time-exit behavior.
