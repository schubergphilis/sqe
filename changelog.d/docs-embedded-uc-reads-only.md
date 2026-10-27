### Fixed

- The Unity Catalog interop docs no longer claim the embedded CLI runs DML on an attached catalog

    It only reads one: UPDATE, DELETE, MERGE and ALTER are refused, and
    CREATE TABLE takes plain columns only. Writes go through the coordinator.
