### Fixed

- The license gate passes again on base64, flatbuffers and lz4_flex

    deps.dev has no record of base64 0.22.1 and 0.23.1, flatbuffers
    25.12.19 or lz4_flex 0.14.0, so the license check reported "license
    evidence missing" for each. The shared CI pin moves to the aikido
    revision that records their reviewed licenses (base64: MIT OR
    Apache-2.0, flatbuffers: Apache-2.0, lz4_flex: MIT).
