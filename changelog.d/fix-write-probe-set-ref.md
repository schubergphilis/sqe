### Fixed

- The write check before the first write into a table with no snapshot asks for the same rights as the write itself

    It used to send a removal of the absent `main` ref, which a catalog authorizes as removing a snapshot ref. A grant that confers appending and setting refs but not removing them was refused on its first CTAS or INSERT. The check now sends what a first append sends (`add-snapshot` and `set-snapshot-ref` of `main`) under the same requirement that can never match, so nothing is committed.
