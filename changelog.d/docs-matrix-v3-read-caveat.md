### Fixed

- The Iceberg matrix's V3 read cell no longer says geometry is unsupported or that shredded variants are read unshredded (#870)

    Both were true when the caveat was written. Geometry is full since #863, and a query reads only the shredded `typed_value` fields it names (#870).
