### Security

- A warehouse found by `catalog_discovery = "polaris-auto"` no longer inherits the template catalog's engine credential identity, and an engine-identity catalog addressed by its `warehouse` name now denies `select` by default like its config name. Both paths could read rows with the engine's keys without a `select` grant (#940).
