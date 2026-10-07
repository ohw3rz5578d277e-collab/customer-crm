# Member consent foundation contract

## Purpose

Registration is not complete until the Member explicitly accepts the current Terms of Service and Privacy Policy.

## Evidence

Each consent record identifies:

- Member Identity
- Terms version and SHA-256 document digest
- Privacy Policy version and SHA-256 document digest
- acceptance timestamp

Consent must be explicit boolean true. Truthy strings such as "true" are not sufficient evidence.

Consent history is append-only. A new document version creates a new consent record; prior consent evidence is not updated or deleted.

## Re-consent

If the current Terms or Privacy Policy version differs from the latest version accepted by the Member, re-consent is required before the relevant Member flow is considered fully ready.

The exact product behavior while re-consent is pending will be defined with the UI/runtime integration phase.

## Customer and Prospect continuity

Consent attaches to stable Member Identity, so it survives Prospect -> Customer promotion without rewriting historical consent.

Customer ID is not generated or changed by consent.

## Document publication

This foundation stores document identity/version evidence only. Final Terms and Privacy Policy text must describe the actually implemented Google/Cloudflare/Member data flows and should receive qualified legal review before public launch.

## Authorization boundary

This phase does not publish legal documents, write Production consent rows, deploy, change D1 schema, call Google/GAS, mutate CRM customers, generate/change Customer IDs, send LINE, change routes/secrets/R2/UI, write BLACK/MEMORY state, activate commerce, or spend money.
