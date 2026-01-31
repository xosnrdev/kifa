<p align="center">
  <h2 align="center">Kifa (KEE-FAH)</h2>

  <p align="center">
    <b>Crash-proof local logging for POS and mobile money systems.</b>
  </p>
</p>

This project is currently under active development. Use the checklist below to track progress and feature completeness.

- [x] Setup project structure
- [x] Storage engine
  - [x] WAL for crash recovery
  - [x] Memtable for in-memory data storage
  - [x] SSTable for on-disk data storage
  - [x] Compaction for optimizing storage
  - [x] Flush mechanism to persist data from memtable to SSTable
- [ ] CLI / User interface
  - [x] Sources / Ingesters for data input
    - [x] STDIN
    - [x] File tailing
    - [x] TCP socket
    - [x] UDP socket
  - [x] Query/Export Interface for data retrieval
    - [x] LSN-range queries
    - [x] Time-range queries
    - [x] Export to TEXT, JSON, CSV, HEX formats
  - [x] Configuration / Flags for customizing behavior
    - [x] Storage paths
    - [x] Performance tuning options
  - [ ] Web UI for log visualization
- [ ] Testing & Documentation
- [x] Demo
- [ ] Release & Distribution
