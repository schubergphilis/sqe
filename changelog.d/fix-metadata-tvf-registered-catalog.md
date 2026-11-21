### Fixed

- A metadata table function's access check keys the catalog under its registered name

    `table_files('ns', 't', 'CAT')` resolves the catalog case-insensitively and reads `cat`; the policy check now keys `cat` too, not the spelling the call used. Ranger already matched both spellings, so this changes no result under Ranger today.
