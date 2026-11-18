### Fixed

- `element_at(array, i)` accepts an `integer` index again

    Integer literals became `integer`, and `element_at(arr, 1)` failed with
    `array index must be bigint`. Any integer index is now widened to
    `bigint`, as Trino does.
