import java.lang.reflect.Method;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * JDBC conformance probe: creates a table and a view in {@code <schema>}, then
 * calls every no-argument DatabaseMetaData method, the catalog/schema/table/
 * column listings, typed reads and prepared statements. Usage (README.md):
 *   java --add-opens=java.base/java.nio=ALL-UNNAMED -cp <driver.jar> Matrix.java <url> <schema> [key=value ...]
 * Prints one line per check: OK / FAIL / VAL / NOTE. One failure never stops the rest.
 */
public class Matrix {
    static Connection c;
    static DatabaseMetaData md;
    static String S;

    interface RsCall { ResultSet run() throws Exception; }
    interface Run { String run() throws Exception; }

    static void rs(String label, RsCall call) {
        try (ResultSet r = call.run()) {
            ResultSetMetaData m = r.getMetaData();
            int n = m.getColumnCount();
            List<String> rows = new ArrayList<>();
            int count = 0;
            while (r.next()) {
                count++;
                if (count <= 6) {
                    StringBuilder b = new StringBuilder();
                    for (int i = 1; i <= Math.min(n, 6); i++) b.append(r.getString(i)).append('|');
                    rows.add(b.toString());
                }
            }
            System.out.println("OK   " + label + " cols=" + n + " rows=" + count + (rows.isEmpty() ? "" : " " + rows));
        } catch (Throwable e) {
            System.out.println("FAIL " + label + " -> " + oneLine(e));
        }
    }

    static void one(String label, Run run) {
        try {
            System.out.println("OK   " + label + " " + run.run());
        } catch (Throwable e) {
            System.out.println("FAIL " + label + " -> " + oneLine(e));
        }
    }

    static String oneLine(Throwable e) {
        Throwable t = e;
        if (t instanceof java.lang.reflect.InvocationTargetException && t.getCause() != null) t = t.getCause();
        String s = t.getClass().getSimpleName() + ": " + t.getMessage();
        if (t.getCause() != null && t.getCause() != t) s += " <- " + t.getCause().getClass().getSimpleName() + ": " + t.getCause().getMessage();
        return s.replace('\n', ' ');
    }

    static String exec(String sql) throws SQLException {
        try (Statement st = c.createStatement()) {
            boolean has = st.execute(sql);
            if (has) try (ResultSet r = st.getResultSet()) { int n = 0; while (r.next()) n++; return "rows=" + n; }
            return "update=" + st.getUpdateCount();
        }
    }

    public static void main(String[] a) throws Exception {
        String url = a[0];
        S = a[1];
        Properties p = new Properties();
        for (int i = 2; i < a.length; i++) {
            int k = a[i].indexOf('=');
            p.setProperty(a[i].substring(0, k), a[i].substring(k + 1));
        }
        c = DriverManager.getConnection(url, p);
        md = c.getMetaData();
        String cat = c.getCatalog();
        System.out.println("== " + url + " product=" + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion()
                + " driver=" + md.getDriverName() + " " + md.getDriverVersion() + " catalog=" + cat + " schema=" + safeSchema());

        // ---- fixtures -------------------------------------------------------
        String fq = (cat == null || cat.isEmpty()) ? S : cat + "." + S;
        for (String sql : new String[]{
                "DROP VIEW IF EXISTS " + fq + ".v",
                "DROP TABLE IF EXISTS " + fq + ".t",
                "CREATE SCHEMA IF NOT EXISTS " + fq,
                "CREATE TABLE " + fq + ".t (id BIGINT, name VARCHAR, amount DECIMAL(10,2), d DATE, ts TIMESTAMP, "
                        + "b BOOLEAN, f DOUBLE, i INTEGER, bin VARBINARY)",
                "INSERT INTO " + fq + ".t VALUES (1, 'a', 12.34, DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05.123456', "
                        + "true, 1.5, 7, X'0102'), (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)",
                "CREATE VIEW " + fq + ".v AS SELECT id, name FROM " + fq + ".t"}) {
            one("setup " + sql.split(" \\(")[0], () -> exec(sql));
        }

        // ---- every no-arg DatabaseMetaData method ---------------------------
        Method[] ms = DatabaseMetaData.class.getMethods();
        Arrays.sort(ms, Comparator.comparing(Method::getName));
        for (Method m : ms) {
            if (m.getParameterCount() != 0 || m.getName().equals("getConnection")) continue;
            if (ResultSet.class.isAssignableFrom(m.getReturnType())) {
                rs("md." + m.getName() + "()", () -> (ResultSet) m.invoke(md));
            } else {
                try {
                    Object v = m.invoke(md);
                    String s = String.valueOf(v);
                    if (s.length() > 120) s = s.substring(0, 120) + "...";
                    System.out.println("VAL  md." + m.getName() + "() = " + s);
                } catch (Throwable e) {
                    System.out.println("FAIL md." + m.getName() + "() -> " + oneLine(e));
                }
            }
        }
        // supportsConvert / type-specific booleans DBeaver asks
        one("md.supportsResultSetType(FORWARD_ONLY)", () -> "" + md.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        one("md.supportsTransactionIsolationLevel(READ_COMMITTED)", () -> "" + md.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));

        // ---- parameterised catalog calls ------------------------------------
        rs("getSchemas(cat,%)", () -> md.getSchemas(cat, "%"));
        rs("getSchemas(null,S)", () -> md.getSchemas(null, S));
        rs("getTables(null,S,%,null)", () -> md.getTables(null, S, "%", null));
        rs("getTables(cat,S,%,TABLE+VIEW)", () -> md.getTables(cat, S, "%", new String[]{"TABLE", "VIEW"}));
        rs("getTables(cat,S,%,VIEW)", () -> md.getTables(cat, S, "%", new String[]{"VIEW"}));
        rs("getTables(null,null,%,null)", () -> md.getTables(null, null, "%", null));
        rs("getColumns(cat,S,t,%)", () -> md.getColumns(cat, S, "t", "%"));
        rs("getColumns(cat,S,v,%)", () -> md.getColumns(cat, S, "v", "%"));
        rs("getColumns(null,S,%,%)", () -> md.getColumns(null, S, "%", "%"));
        colTypes(cat);
        rs("getPrimaryKeys", () -> md.getPrimaryKeys(cat, S, "t"));
        rs("getImportedKeys", () -> md.getImportedKeys(cat, S, "t"));
        rs("getExportedKeys", () -> md.getExportedKeys(cat, S, "t"));
        rs("getCrossReference", () -> md.getCrossReference(cat, S, "t", cat, S, "v"));
        rs("getIndexInfo", () -> md.getIndexInfo(cat, S, "t", false, true));
        rs("getBestRowIdentifier", () -> md.getBestRowIdentifier(cat, S, "t", DatabaseMetaData.bestRowSession, true));
        rs("getVersionColumns", () -> md.getVersionColumns(cat, S, "t"));
        rs("getTablePrivileges", () -> md.getTablePrivileges(cat, S, "t"));
        rs("getColumnPrivileges", () -> md.getColumnPrivileges(cat, S, "t", "%"));
        rs("getFunctions", () -> md.getFunctions(cat, null, "%"));
        rs("getFunctionColumns", () -> md.getFunctionColumns(cat, null, "%", "%"));
        rs("getProcedures", () -> md.getProcedures(cat, null, "%"));
        rs("getProcedureColumns", () -> md.getProcedureColumns(cat, null, "%", "%"));
        rs("getUDTs", () -> md.getUDTs(cat, null, "%", null));
        rs("getSuperTypes", () -> md.getSuperTypes(cat, null, "%"));
        rs("getSuperTables", () -> md.getSuperTables(cat, null, "%"));
        rs("getAttributes", () -> md.getAttributes(cat, null, "%", "%"));
        rs("getPseudoColumns", () -> md.getPseudoColumns(cat, null, "%", "%"));

        // ---- session: setCatalog / setSchema -------------------------------
        one("setSchema(S)+unqualified", () -> { c.setSchema(S); return safeSchema() + " " + exec("SELECT count(*) FROM t"); });
        if (cat != null) one("setCatalog(cat)", () -> { c.setCatalog(cat); return c.getCatalog(); });

        // ---- type round trip -------------------------------------------------
        typed("table row", "SELECT * FROM " + fq + ".t ORDER BY id");
        typed("literals", "SELECT CAST(1 AS INTEGER) i, CAST(2 AS BIGINT) bi, CAST(1.5 AS DOUBLE) dbl, CAST(3.25 AS REAL) r, "
                + "CAST(12.34 AS DECIMAL(10,2)) dec, CAST('x' AS VARCHAR) s, true bo, DATE '2026-01-02' dt, "
                + "TIMESTAMP '2026-01-02 03:04:05.123456' ts, CAST(NULL AS VARCHAR) nul, X'0A0B' bin, "
                + "CAST(TIME '03:04:05' AS TIME) tm, CAST(1 AS SMALLINT) sm, CAST(1 AS TINYINT) ti");
        typed("timestamptz", "SELECT CAST(TIMESTAMP '2026-01-02 03:04:05 UTC' AS TIMESTAMP WITH TIME ZONE) tz");
        typed("array", "SELECT ARRAY[1,2,3] arr");
        typed("map", "SELECT MAP(ARRAY['a'], ARRAY[1]) m");
        typed("row", "SELECT CAST(ROW(1, 'x') AS ROW(a INTEGER, b VARCHAR)) st");
        typed("json", "SELECT json_parse('{\"a\":1}') js");
        typed("uuid", "SELECT uuid() u");

        // ---- prepared statements --------------------------------------------
        one("prepared ? + 1", () -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT ? + 1 AS x")) {
                ps.setLong(1, 41);
                try (ResultSet r = ps.executeQuery()) { r.next(); return r.getString(1); }
            }
        });
        one("prepared WHERE id = ?", () -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name FROM " + fq + ".t WHERE id = ?")) {
                ps.setLong(1, 1);
                try (ResultSet r = ps.executeQuery()) { r.next(); return r.getString(1); }
            }
        });
        one("prepared varchar/date params", () -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT count(*) FROM " + fq + ".t WHERE name = ? OR d = ?")) {
                ps.setString(1, "a");
                ps.setDate(2, java.sql.Date.valueOf("2026-01-02"));
                try (ResultSet r = ps.executeQuery()) { r.next(); return r.getString(1); }
            }
        });
        one("prepared getMetaData before execute", () -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT id, name FROM " + fq + ".t")) {
                ResultSetMetaData m = ps.getMetaData();
                return m == null ? "null" : m.getColumnCount() + " " + m.getColumnName(1) + ":" + m.getColumnTypeName(1);
            }
        });
        one("prepared getParameterMetaData", () -> {
            try (PreparedStatement ps = c.prepareStatement("SELECT name FROM " + fq + ".t WHERE id = ?")) {
                ParameterMetaData pm = ps.getParameterMetaData();
                return pm.getParameterCount() + " type=" + pm.getParameterType(1) + " " + pm.getParameterTypeName(1);
            }
        });
        one("prepared INSERT ?", () -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + fq + ".t (id, name) VALUES (?, ?)")) {
                ps.setLong(1, 3); ps.setString(2, "c");
                return "update=" + ps.executeUpdate();
            }
        });
        one("batch INSERT", () -> {
            try (PreparedStatement ps = c.prepareStatement("INSERT INTO " + fq + ".t (id, name) VALUES (?, ?)")) {
                for (int i = 10; i < 13; i++) { ps.setLong(1, i); ps.setString(2, "n" + i); ps.addBatch(); }
                return Arrays.toString(ps.executeBatch());
            }
        });

        // ---- statement features DBeaver uses --------------------------------
        one("setMaxRows(1)", () -> {
            try (Statement st = c.createStatement()) {
                st.setMaxRows(1);
                try (ResultSet r = st.executeQuery("SELECT * FROM " + fq + ".t")) { int n = 0; while (r.next()) n++; return "rows=" + n; }
            }
        });
        one("setFetchSize+getMoreResults", () -> {
            try (Statement st = c.createStatement()) {
                st.setFetchSize(200);
                st.execute("SELECT 1");
                return "more=" + st.getMoreResults() + " uc=" + st.getUpdateCount();
            }
        });
        one("DDL via execute", () -> exec("CREATE TABLE IF NOT EXISTS " + fq + ".t2 (x INTEGER)"));
        one("DROP", () -> exec("DROP TABLE IF EXISTS " + fq + ".t2"));
        one("SHOW CATALOGS", () -> exec("SHOW CATALOGS"));
        one("SHOW SCHEMAS", () -> exec("SHOW SCHEMAS"));
        one("SHOW TABLES FROM S", () -> exec("SHOW TABLES FROM " + fq));
        one("SHOW COLUMNS FROM t", () -> exec("SHOW COLUMNS FROM " + fq + ".t"));
        one("DESCRIBE t", () -> exec("DESCRIBE " + fq + ".t"));
        one("EXPLAIN", () -> exec("EXPLAIN SELECT * FROM " + fq + ".t"));
        one("bad SQL is SQLException", () -> {
            try { exec("SELEC 1"); return "no error!"; } catch (SQLException e) { return "SQLState=" + e.getSQLState(); }
        });
        one("cancel", () -> {
            try (Statement st = c.createStatement()) {
                ScheduledExecutorService ex = Executors.newSingleThreadScheduledExecutor();
                ex.schedule(() -> { try { st.cancel(); } catch (SQLException ignored) { } }, 500, TimeUnit.MILLISECONDS);
                long t0 = System.nanoTime();
                try (ResultSet r = st.executeQuery("SELECT count(*) FROM range(0, 3000000000) a")) {
                    r.next();
                    return "finished uncancelled in " + (System.nanoTime() - t0) / 1_000_000 + "ms";
                } catch (SQLException e) {
                    return "cancelled after " + (System.nanoTime() - t0) / 1_000_000 + "ms: " + e.getMessage().split("\n")[0];
                } finally { ex.shutdownNow(); }
            }
        });
        one("autocommit", () -> "" + c.getAutoCommit());
        one("isValid", () -> "" + c.isValid(5));

        // ---- cleanup --------------------------------------------------------
        one("cleanup", () -> exec("DROP VIEW IF EXISTS " + fq + ".v") + " " + exec("DROP TABLE IF EXISTS " + fq + ".t")
                + " " + exec("DROP SCHEMA IF EXISTS " + fq));
        c.close();
    }

    static String safeSchema() {
        try { return c.getSchema(); } catch (Throwable e) { return "ERR(" + oneLine(e) + ")"; }
    }

    static void colTypes(String cat) {
        try (ResultSet r = md.getColumns(cat, S, "t", "%")) {
            StringBuilder b = new StringBuilder();
            while (r.next()) {
                b.append(r.getString("COLUMN_NAME")).append(':').append(r.getString("TYPE_NAME")).append('/')
                        .append(r.getInt("DATA_TYPE")).append(" size=").append(r.getString("COLUMN_SIZE"))
                        .append(" dec=").append(r.getString("DECIMAL_DIGITS")).append(" null=").append(r.getString("IS_NULLABLE"))
                        .append("; ");
            }
            System.out.println("OK   getColumns types " + b);
        } catch (Throwable e) {
            System.out.println("FAIL getColumns types -> " + oneLine(e));
        }
    }

    static void typed(String label, String sql) {
        try (Statement st = c.createStatement(); ResultSet r = st.executeQuery(sql)) {
            ResultSetMetaData m = r.getMetaData();
            StringBuilder b = new StringBuilder();
            boolean first = r.next();
            for (int i = 1; i <= m.getColumnCount(); i++) {
                b.append(m.getColumnLabel(i)).append(':').append(m.getColumnTypeName(i)).append('/').append(m.getColumnType(i))
                        .append(" p=").append(m.getPrecision(i)).append(" s=").append(m.getScale(i))
                        .append(" cls=").append(m.getColumnClassName(i));
                if (first) {
                    try {
                        Object o = r.getObject(i);
                        b.append(" obj=").append(o == null ? "null" : o.getClass().getSimpleName() + "(" + o + ")");
                        b.append(" str=").append(r.getString(i));
                    } catch (Throwable e) {
                        b.append(" GETERR=").append(oneLine(e));
                    }
                }
                b.append("; ");
            }
            System.out.println("OK   typed " + label + " " + b);
        } catch (Throwable e) {
            System.out.println("FAIL typed " + label + " -> " + oneLine(e));
        }
    }
}
