### Added

- Flight SQL bulk ingest: documented limits and end-to-end proof for snapshot properties and refusals (#904)

    The connecting page now documents the `snapshot-property.<key>` ingest option and its key rules. It also covers the 4 MiB per gRPC message limit (tonic's default; one row larger than that fails the whole ingest with `OutOfRange` and leaves the table unchanged), admission like a write statement, and the write-access refusal. New integration tests check that the option lands in the snapshot summary in every ingest mode, in a transaction and outside one, and applies to one request only. A reserved key is refused before any snapshot. `SET SNAPSHOT_PROPERTIES` reaches the summary of CTAS, INSERT, UPDATE, DELETE, MERGE and INSERT OVERWRITE. A new Ranger access-control test checks that an ingest by a principal without write access is refused with one failure audit record. The ingest reply still carries only the row count; see the merge request for the snapshot-id options.

### Fixed

- A Flight SQL ingest whose upload breaks in transport now fails with the transport's own gRPC status (#904)

    A `FlightData` message over the coordinator's 4 MiB decoding limit failed the ingest as `Internal` / `EXECUTION_FAILED` with a nested "Stream error: External error: Tonic error: ..." message. The ingest now keeps the first error the upload stream yields and answers with that status: `OutOfRange` with tonic's "decoded message length too large" message. The audit record carries `INVALID_ARGUMENTS`.
