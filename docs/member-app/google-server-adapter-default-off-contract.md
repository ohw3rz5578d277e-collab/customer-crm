# Google server adapter default-off contract

## Roadmap phase

This adapter follows the independent-backup contract and precedes any real GAS/Google deployment.

It is also the server-envelope boundary used by Member backend sequence step 5. Source remains default-off and performs no network request.

## Boundary

Browser clients never receive Google integration credentials and never call the privileged Google write endpoint directly.

Cloudflare/server code may later authenticate to a GAS or Google API adapter using managed server-side credentials.

## Request authentication

The candidate HMAC envelope binds:

- exact HTTP method `POST`
- exact endpoint path `/member/profile-sync`
- safe-integer timestamp
- unique URL-safe nonce, 22–128 characters
- `sync_event_id`
- SHA-256 digest of canonical request body

The managed shared secret must be at least 32 characters.

Maximum accepted clock skew is 300000 ms (five minutes). A caller cannot expand the verification window beyond that limit.

Verification rejects stale timestamps, malformed scalar/time evidence, unsupported destination/path, nonce replay, malformed event IDs, malformed canonical bodies, weak/missing secret evidence, and invalid signatures.

Event replay semantics remain governed by exact sync-event/profile version/digest rules; an already-seen event must never be blindly executed.

## Canonical body

Canonical JSON recursively sorts object keys. Unsupported values such as `undefined`, function, symbol, bigint, and non-finite numbers fail closed instead of being silently omitted or transformed.

## Default off

The source adapter only constructs/verifies envelopes. It performs no network request.

Any real Google request executor must require a separate runtime feature flag/secret, exact approved destination, bounded operation type, exact approved release SHA, and explicit Production authorization.

## Secret handling

Shared secrets/API credentials must be held in managed server-side secret storage. They must not appear in source, Sheets, Customer History, browser JavaScript, logs, review payloads, or backup exports.

## Authorization boundary

No Google/GAS network call, spreadsheet/Drive write, credential/secret creation or activation, Production D1 write, CRM mutation, Customer ID mutation, LINE send, deploy, route/R2/UI change, BLACK/MEMORY write, commerce activation, or paid spend is authorized.
