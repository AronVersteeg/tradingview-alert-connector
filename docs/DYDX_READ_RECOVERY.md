# dYdX indexer read recovery

Account and market GET requests made by the managed dYdX client use a bounded
read-only transport. No additional environment variables are required.

- Each attempt has an 8-second Axios timeout and an AbortController deadline,
  including connection setup. The connection is canceled on timeout.
- At most three attempts are made per read, within a 30-second total budget.
- Transient connection resets/timeouts and HTTP 408, 429, 500, 502, 503, 504
  can be retried with exponential backoff and jitter.
- Retry-After is respected. If it exceeds the remaining budget, the read fails
  without issuing an early retry.
- Other HTTP errors and unrelated errors fail immediately.
- Exhausted reads throw to the existing caller; an unavailable account is not
  substituted with a cached or empty/flat account.
- Account/position, order-list and market reads all use this transport, including
  reads made after reconnecting the dYdX clients.

The installed SDK's RestClient accepts apiTimeout but does not forward it to
Axios. The integration therefore replaces only GET on this client's account and
market module instances, retaining SDK endpoint and query construction. It does
not change global Axios defaults, SDK prototypes, transaction submission,
cancellation, the composite client, or trading rules.

Logs distinguish `dYdX indexer read retry scheduled.`, `dYdX indexer read recovered.`
and `dYdX indexer read failed.` with attempt counts, elapsed time and status/code.
Recovery logs redact the wallet address and omit request headers and payloads.
Existing higher-level failure handling remains in place if recovery is exhausted;
this is not a guarantee against extended indexer outages or stale successful data.

Tests cover transient/permanent errors, Retry-After, exhausted budgets, real
stalled HTTP connections, SDK query compatibility, account/order reconciliation,
and preserving an already reached position before fail-safe flattening.
