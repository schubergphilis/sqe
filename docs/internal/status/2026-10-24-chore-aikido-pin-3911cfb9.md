**STATUS 2026-10-24: aikido pin moves to 3911cfb9 so `ci-deps-image` builds again (`chore/aikido-pin-3911cfb9`).**

The 5 aikido include refs and `AIKIDO_SHARED_REF` move from ac257d20 to
3911cfb9, aikido main after !243 (deps image pruned without Python; job
5660962 failed on the Python helper) and the tools-digest promote
af037cbc49e4. Also in the range: renovate bumps of `python:3.14-slim` and
trivy 0.76.1. The aikido main pipeline for 3911cfb9 is green (pipeline
1679286). No aikido tag is needed: the pin is a commit.

**NEXT:** unchanged
