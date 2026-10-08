# Member lifecycle cross-contract security gate

This regression gate binds the new Prospect/Member lifecycle to the existing FAMILY PASS contracts.

It must continue to prove:

- Prospect registration never generates canonical Customer ID.
- Prospect cannot read Customer/Family MEMORIES, Passport, TODAY'S MEMORY, NEXT MEMORY, FAMILY PASS, or BLACK state.
- Prospect promotion fails closed on Member/Prospect binding mismatch.
- Google profile planning remains network-disabled.
- ambiguous profile changes cannot write Master.
- D1 PII is inaccessible at the hard 30-day deadline.
- signup benefit cannot be redeemed twice.
- Customer FAMILY PASS still reaches BLACK at exactly 10 qualifying MEMORIES.
- BLACK remains non-automatic and its product contract remains photo goods 10%, not shooting-fee discount.
- all covered planners/read models remain non-writing.

This gate does not authorize Production operations.
