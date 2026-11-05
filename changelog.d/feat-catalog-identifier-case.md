### Added

- Per-catalog `identifier_case = "upper"` and `ATTACH ... (IDENTIFIER_CASE 'upper')` fold unquoted names the Snowflake way

    A Snowflake Horizon table is stored as `SALES.ORDERS` with column `ID`,
    while SQE folds unquoted names to lower case, so it had to be written
    `sf."SALES"."ORDERS"` and `"ID"`. With the option set, the front door
    folds unquoted names for that catalog to upper case before planning and
    before policy rewriting: schema and table parts always, columns, aliases
    and CTE names when every table in the statement is in an upper-case
    catalog, and `USE sf.sales` folds its schema. Quoted names, function and type names, `information_schema` and
    metadata-table columns are left alone. Masks bind case-insensitively as
    before; a row-filter expression on a Snowflake column quotes it, and an
    unquoted one fails the query rather than dropping the filter.
