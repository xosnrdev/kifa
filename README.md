<p align="center">
  <h2 align="center">Kifa (KEE-FAH)</h2>

  <p align="center">
    <b>Crash-proof local logging for POS and mobile money systems.</b>
  </p>
</p>

This project is currently under active development. Use the checklist below to track progress and feature completeness.

- [x] Setup project structure
- [ ] Storage engine
  - [x] WAL for crash recovery
  - [x] Memtable for in-memory data storage
  - [x] SSTable for on-disk data storage
  - [x] Compaction for optimizing storage
  - [ ] Flush mechanism to persist data from memtable to SSTable
- [ ] CLI / User interface
  - [ ] Sources / Ingesters for data input
    - [ ] STDIN
    - [ ] File tailing
    - [ ] TCP socket
    - [ ] UDP socket
  - [ ] Query/Export Interface for data retrieval
    - [ ] LSN-range queries
    - [ ] Time-range queries
    - [ ] Export to TEXT, JSON, CSV, HEX formats
  - [ ] Configuration / Flags for customizing behavior
    - [ ] Storage paths
    - [ ] Performance tuning options
  - [ ] Web UI for log visualization
- [ ] Testing & Documentation
- [ ] Demo
- [ ] Release & Distribution
