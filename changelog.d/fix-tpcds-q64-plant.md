### Fixed

- TPC-DS `q64` returns rows on generated data at every scale (#785)

    `q64` needs one item, priced 65 to 74 in one of six colours, that a store sold and had returned in both 1999 and 2000, that also sits in the catalog sales whose list price beats twice their refunds, and that was sold under a demographic whose marital status differs from the buyer's current one. About five such pairs exist at SF1 on random data, and our seed had none, so the query came back empty on both SQE and Trino (official SF1: 2 rows). The generator now plants that coincidence like the `q17`, `q24`, `q25` and `q29` ones: item 6, store tickets 7 and 8 at store 1, customer 7, and catalog order 7 with a small return. TPC-DS data must be regenerated.
