**STATUS 2026-10-23: aikido pin moves to ac257d20 for the ctutils 0.4.x license evidence (`ci/aikido-pin-ctutils`).**

The 5 aikido include refs and `AIKIDO_SHARED_REF` move from 96527c4a to
ac257d20, aikido main after !241 (reviewed license evidence for
`pkg:cargo/ctutils@0.4.*`) and the tools-digest promote 514ec012eb42. The
aikido main pipeline for ac257d20 is green (pipeline 1677685). No aikido tag
is needed: the pin is a commit.

The `.asset` reputation waiver for `cargo/ctutils` (until 2026-11-06) stays
until deps.dev lists ctutils 0.4.5.

**NEXT:** drop the `.asset` ctutils waiver once deps.dev indexes 0.4.5.
