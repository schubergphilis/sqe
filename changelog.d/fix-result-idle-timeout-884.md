### Fixed

- A long query is no longer cut by the result idle timeout while the client waits for it, and a client that stops reading is dropped (#884)

    `query.stream_idle_timeout_secs` aborted a query whose plan produced no batch for 300 s unless its operator metrics moved. A multi-level sort merge reports its intermediate passes to a detached metric set, so a single-row SF100 sort under concurrent load was cut at about 600 s while still executing. With a `query.timeout_secs` deadline (the default) the deadline alone now bounds a running plan; the idle guard on the plan applies only when `timeout_secs = 0`.

    The idle timeout also never saw a client that stopped reading: the check ran only when something polled the result, and an HTTP/2 client with a full window is not polled. A task now pulls each streaming result into a one-slot channel. When a batch is ready and the client has not taken the previous one within the idle timeout, the query fails with "the client did not read the next result batch" and its permits, memory reservation and spill files are released; the client reads that error after the buffered batch. A client that disconnects still cancels the query at once.
