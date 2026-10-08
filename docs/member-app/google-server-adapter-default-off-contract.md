# Google server adapter default-off contract

## Roadmap phase

This adapter follows the independent-backup contract and precedes any real GAS/Google deployment.

## Boundary

Browser clients never receive Google integration credentials and never call the privileged Google write endpoint directly.

Cloudflare/server code may later authenticate to a GAS or Google API adapter using managed server-side credentials.

## Request authentication

The candidate HMAC envelope binds:

- HTTP method
- endpoint path
- timestamp
- unique nonce
- sync_event_id
- SHA-256 digest of canonical request body

Verification must reject stale timestamps, nonce replay, invalid signatures, and malformed event IDs.

Event replay semantics remain governed by profile version/digest rules; an already-seen event must never be blindly executed.

## Default off

The source adapter only constructs/verifies envelopes. It performs no network request.

Any real Google request executor must require a separate runtime feature flag/secret, exact approved destination, bounded operation type, and explicit Production authorization.

## Secret handling

Shared secrets/API credentials must be held in managed server-side secret storage. They must not appear in source, Sheets, Customer History, browser JavaScript, logs, review payloads, or backup exports.

## Authorization boundary

No Google/GAS network call, spreadsheet/Drive write, credential/secret creation or activation, Production D1 write, CRM mutation, Customer ID mutation, LINE send, deploy, route/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
