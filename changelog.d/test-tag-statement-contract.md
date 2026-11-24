### Changed

- Tests pin the SHOW TAGS columns and the tag and policy refusal texts

    The SHOW TAGS schema (`column`, `tag`, `value`, `applied`, in that order) and the refusal phrases clients match (`mask conflict on ...: more than one distinct column mask`, `row filter conflict on ...: more than one distinct row filter`, `tag change refused: it would create a ... conflict`, `requires apply_tag`, `requires remove_tag`, `tag_removal = "admin"`) are now asserted in full, so changing one fails a test instead of breaking a client. Tests only; no output change.
