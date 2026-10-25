### Added

- Distributed workers prune row groups on shredded variant fields (#870)

    A `variant_get(v, '$.path', '<type>') <op> literal` filter now skips row
    groups on the worker as well as on the coordinator. The coordinator sends
    the prune as plain data in the scan ticket (new field `variant_prunes`,
    ticket version 5; the field id is resolved first, so a renamed column
    still prunes). Tickets without it keep their older version and bytes.
    The coordinator still refuses workers that report an older ticket version
    at hello, so upgrade workers before the coordinator.
