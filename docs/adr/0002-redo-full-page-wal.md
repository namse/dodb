# ADR 0002: Redo-only full-page after-image WAL

Status: accepted

The future durability layer will use redo-only WAL containing full-page
after-images, with NO-STEAL and NO-FORCE. ARIES is not the target design.

Phase 0 only defines page LSN/checksum fields and a durable-file seam. It does
not write WAL records or implement recovery.

