# Storage format foundation

The fixed page size is 4096 bytes. Phase 0 defines codecs only.

## Common page header

Each encoded page is exactly 4096 bytes and uses little-endian numeric fields:

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 4 | magic `DBPG` |
| 4 | 2 | format version |
| 6 | 1 | page type (`Internal`, `Leaf`, `Overflow`, `Free`, `Test`) |
| 7 | 1 | reserved, must be zero |
| 8 | 8 | page id |
| 16 | 8 | page LSN |
| 24 | 4 | flags |
| 28 | 4 | CRC32C checksum |
| 32 | 4064 | body, currently opaque/zero-filled |

CRC32C is calculated over the entire page after zeroing bytes 28..32. The
codec validates exact length, magic, version, page type, checksum, and an
optional expected physical page id.

## Double superblock

Page 0 is slot A and page 1 is slot B. Each is a full-page explicit codec with
magic `DSBK`, version, generation, database UUID, tenant/shard identity and
epoch, page size, nullable root/free-list/high-water page references,
checkpoint LSN, and CRC32C. A nullable page reference is encoded as
`u64::MAX`.

On open, unsupported format is an explicit error. Otherwise the valid copy
with the highest generation is selected. Both invalid copies are corruption.
Generation wraparound is deliberately outside the Phase 0 operating
assumptions.

No allocator, free list, root page, leaf/internal format, overflow format, WAL,
or checkpoint protocol is implemented here.

