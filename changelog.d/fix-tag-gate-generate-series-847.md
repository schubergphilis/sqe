### Fixed

- `generate_series()` and `range()` answer under a tag-aware policy store (#847)

    Under Ranger every query over DataFusion's built-in generator table
    functions was refused with "tag state unknown": the tag gate did not
    recognise their provider. They have no table and no column tags, so the
    gate now answers them as untagged, matched by provider type. Any other
    unrecognised provider is still refused.
