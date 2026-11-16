### Changed

- Release images are multi-arch: a `vX.Y.Z` tag now publishes one manifest list with linux/amd64 and linux/arm64 under `:vX.Y.Z` and `:stable`, in the GitLab registry and Harbor, so arm64 nodes and Apple silicon run the release image natively. The binaries are cross-compiled on the amd64 runners with cargo-zigbuild (glibc 2.28 floor), both platforms are scanned, and every published tag is read back and refused unless it lists both platforms under the release's index digest. The GitLab Release names that digest. Dev images from `main` (`:<short-sha>`) stay amd64 only
