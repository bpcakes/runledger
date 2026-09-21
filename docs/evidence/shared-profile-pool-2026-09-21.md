# Shared PostgreSQL profile owner

Companion to Batter `batter-979` and PR #3. Review base: merged PR #21,
`a8f62833d4801c342b4e4244aeae8607fc26ae35`.

`RunledgerDatabase` delegates its pool/profile storage, direct configuration probe,
and all three mandatory native hooks to `batter_sqlx::PgProfiledPool`. Public
constructors/accessors, one-authoritative-schema validation and native queue,
migration and atomic contracts remain unchanged. This prevents copying the same
hook lifecycle into Batter's authentication-attempt adapter. Native admission
and owned completion can now share one profile owner without inferred hook state.
The foundation remains independent of Runledger and Runlimit.

The reviewed-source pin advances to the shared foundation checkpoint. The
source-guard regression now reads its adapter pin from the fixture manifest,
instead of assuming a historical hash. It still mutates that pin and proves
adapter-only changes do not change the foundation contract.

Focused validation: six live database-profile tests passed on PostgreSQL 18.6,
including native fast acquisition and failed-release retirement. Full package,
consumer smoke and native review results are pending.
