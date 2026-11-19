# Entry-Only Notifications

The application sends email only for confirmed manual, Purple or Green entries,
for both directions and every supported pair. SMTP configuration is unchanged.
Automatic entry mail retains `OPEN_LIQUIDITY_V2_PURPLE_TRIGGER_EMAIL_ENABLED`
(default true); Manual entry mail uses its existing entry handler.

No email is sent for raw/filtered intrusions, observational Shadow signals,
scanner/RSI events, TP hits, skipped signals, rejected orders, uncertain submissions
or fail-safe-flattened attempts. The shared SMTP sender requires explicit confirmed
entry metadata; a matching subject alone never allows an email. This policy also
overrides old informational env values set to true. No new env is required.

Monitoring, intrusion history, gap/fractal data, overlays, entry decisions and
protective SL/TP/trailing execution continue unchanged. Disabling intrusion mail
does not disable the Yellow signals used by automatic trailing stops. Historical
SMTP timestamps remain stored; new observations do not invent a sent timestamp.

Automatic messages retain persisted retries and per-signature deduplication.
Old queued non-entry messages are never delivered. A queued uncertain signal can
become deliverable only after the matching order intent is confirmed. No unrelated
historical order is backfilled as a fresh notification. SMTP acceptance is not proof
of inbox delivery; a crash after acceptance but before persisting its timestamp
can still cause a duplicate retry.

Offline tests mock exchange and SMTP and use temporary state files. No live order
or test email is required to verify the policy.
