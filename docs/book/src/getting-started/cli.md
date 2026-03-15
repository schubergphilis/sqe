# Using the CLI

`sqe-cli` is an interactive SQL client that connects to an SQE coordinator via Arrow Flight SQL.

## Usage

```
sqe-cli [OPTIONS]

Options:
  -H, --host <HOST>          Coordinator host [default: localhost]
  -p, --port <PORT>          Coordinator port [default: 50051]
      --protocol <PROTOCOL>  Wire protocol: flight or http [default: flight]
  -u, --user <USER>          Username (prompts if not set)
      --token <TOKEN>        Bearer token (skips password flow)
  -e, --execute <SQL>        Execute a single query and exit
  -f, --format <FORMAT>      Output format: table, csv, json [default: table]
      --tls                  Use HTTPS/TLS
      --insecure             Accept invalid TLS certificates
  -h, --help                 Print help
  -V, --version              Print version
```

## Interactive Mode

```bash
sqe-cli --host sqe-coordinator --port 50051 --user alice
```

```
Password: ****
sqe-cli 0.1.0 connected to http://sqe-coordinator:50051 (flight)
Type SQL queries, or \q to quit. End multi-line queries with ;

sqe> SELECT * FROM raw.orders LIMIT 3;
 order_id | customer_id | amount | region
----------+-------------+--------+--------
 1        | 100         | 250.00 | EU
 2        | 101         | 150.00 | US
 3        | 100         | 300.00 | EU
(3 rows)

sqe> \q
```

### Multi-line Queries

Queries are executed when you type `;`:

```
sqe> SELECT
  ->   region,
  ->   COUNT(*) AS orders,
  ->   SUM(amount) AS total
  -> FROM raw.orders
  -> GROUP BY region
  -> ORDER BY total DESC;
```

### Commands

| Command | Action |
|---|---|
| `\q` | Quit |
| `quit` | Quit |
| `exit` | Quit |
| `Ctrl+C` | Cancel current input / quit |
| `Ctrl+D` | Quit (EOF) |

History is saved to `~/.sqe_history`.

## Single Query Mode

Execute one query and exit — useful for scripts:

```bash
sqe-cli -H localhost -p 50051 -u alice -e "SELECT COUNT(*) FROM raw.orders;"
```

## Output Formats

### Table (default)

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format table
```
```
 a | b
---+-------
 1 | hello
(1 rows)
```

### CSV

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format csv
```
```
a,b
1,hello
```

### JSON (newline-delimited)

```bash
sqe-cli -e "SELECT 1 AS a, 'hello' AS b;" --format json
```
```json
{"a":"1","b":"hello"}
```

## Authentication

### Username/Password

```bash
# Interactive prompt
sqe-cli --user alice

# Environment variables (no prompts)
export SQE_USER=alice
export SQE_PASSWORD=secret
sqe-cli -e "SHOW SCHEMAS;"
```

### Bearer Token

Skip the password flow entirely with a pre-obtained token:

```bash
sqe-cli --token eyJhbGciOiJSUzI1NiIs... -e "SELECT 1;"
```

## Connecting in Kubernetes

```bash
# Port-forward to the coordinator
kubectl port-forward svc/sqe-coordinator 50051:50051

# Then connect locally
sqe-cli --host localhost --port 50051

# Or exec directly into the pod
kubectl exec -it deploy/sqe-coordinator -- sqe-cli
```

## Using with Trino Protocol

For compatibility with tools that speak Trino HTTP:

```bash
sqe-cli --protocol http --host localhost --port 8080 --user alice
```

This uses the Trino-compatible `/v1/statement` endpoint instead of Flight SQL.
