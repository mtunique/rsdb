#!/usr/bin/env python3
"""
RSDB FlightSQL Client

Connect to RSDB server and run SQL queries via Arrow FlightSQL protocol.

Usage:
    # Interactive mode
    python scripts/flight_client.py

    # Execute a single query
    python scripts/flight_client.py --sql "SELECT 1 + 1 AS result"

    # Connect to a different host
    python scripts/flight_client.py --host localhost --port 8819

Requirements:
    pip install pyarrow adbc-driver-flightsql adbc-driver-manager
"""

import argparse
import sys


def connect_adbc(host: str, port: int):
    """Connect using ADBC FlightSQL driver (recommended)."""
    try:
        import adbc_driver_flightsql.dbapi as flight_sql

        conn = flight_sql.connect(uri=f"grpc://{host}:{port}")
        return conn, "adbc"
    except ImportError:
        return None, None


def connect_pyarrow(host: str, port: int):
    """Connect using PyArrow Flight client (fallback)."""
    try:
        import pyarrow.flight as flight

        client = flight.FlightClient(f"grpc://{host}:{port}")
        return client, "pyarrow"
    except ImportError:
        return None, None


def print_arrow_table(table):
    """Print an Arrow table in a formatted way."""
    try:
        print(table.to_pandas().to_string(index=False))
    except ImportError:
        # pandas not installed, print manually
        schema = table.schema
        col_names = [field.name for field in schema]
        widths = [len(name) for name in col_names]

        # Compute column widths
        for i, col in enumerate(table.columns):
            for val in col:
                widths[i] = max(widths[i], len(str(val.as_py())))

        # Print header
        header = " | ".join(name.ljust(widths[i]) for i, name in enumerate(col_names))
        print(header)
        print("-+-".join("-" * w for w in widths))

        # Print rows
        for row_idx in range(table.num_rows):
            row = " | ".join(
                str(table.column(i)[row_idx].as_py()).ljust(widths[i])
                for i in range(len(col_names))
            )
            print(row)


def execute_adbc(conn, sql: str):
    """Execute SQL via ADBC connection."""
    cursor = conn.cursor()
    cursor.execute(sql)
    try:
        table = cursor.fetch_arrow_table()
        print_arrow_table(table)
        print(f"\n({table.num_rows} rows)")
    except Exception as e:
        if "UNKNOWN" in str(e) or "Fetched" in str(e):
            print("Query executed successfully (no results).")
        else:
            raise
    cursor.close()


def execute_pyarrow(client, sql: str):
    """Execute SQL via PyArrow Flight client."""
    import pyarrow.flight as flight
    from pyarrow.flight import FlightDescriptor

    # Use FlightSQL CommandStatementQuery
    try:
        import pyarrow.flight as pf

        # Encode a FlightSQL CommandStatementQuery
        # The FlightSQL protocol encodes commands as protobuf Any messages
        # For raw Flight, we can use descriptor-based approach
        descriptor = FlightDescriptor.for_command(sql.encode("utf-8"))
        info = client.get_flight_info(descriptor)

        # Read from all endpoints
        for endpoint in info.endpoints:
            reader = client.do_get(endpoint.ticket)
            table = reader.read_all()
            print(table.to_pandas().to_string(index=False))
            print(f"\n({table.num_rows} rows)")
            return
        print("Query executed successfully (no results).")
    except Exception as e:
        # If FlightSQL doesn't work, try a simpler approach
        print(f"Error: {e}")


def main():
    parser = argparse.ArgumentParser(description="RSDB FlightSQL Client")
    parser.add_argument("--host", default="localhost", help="Server host")
    parser.add_argument("--port", type=int, default=8819, help="Server port")
    parser.add_argument("--sql", help="SQL query to execute")
    parser.add_argument(
        "--driver",
        choices=["adbc", "pyarrow", "auto"],
        default="auto",
        help="Connection driver",
    )
    args = parser.parse_args()

    # Connect
    conn = None
    driver = None

    if args.driver in ("auto", "adbc"):
        conn, driver = connect_adbc(args.host, args.port)
        if conn:
            print(f"Connected to grpc://{args.host}:{args.port} via ADBC FlightSQL")

    if conn is None and args.driver in ("auto", "pyarrow"):
        conn, driver = connect_pyarrow(args.host, args.port)
        if conn:
            print(f"Connected to grpc://{args.host}:{args.port} via PyArrow Flight")

    if conn is None:
        print("Error: Could not connect. Install one of:")
        print("  pip install adbc-driver-flightsql adbc-driver-manager")
        print("  pip install pyarrow")
        sys.exit(1)

    # Execute single query or enter interactive mode
    if args.sql:
        if driver == "adbc":
            execute_adbc(conn, args.sql)
        else:
            execute_pyarrow(conn, args.sql)
    else:
        # Interactive mode
        print('Type SQL queries (end with ;). Type "quit" to exit.\n')
        buffer = []
        while True:
            try:
                prompt = "rsdb> " if not buffer else "   -> "
                line = input(prompt)

                if line.strip().lower() in ("quit", "exit", "\\q"):
                    break

                buffer.append(line)
                full = " ".join(buffer)

                if full.rstrip().endswith(";"):
                    sql = full.rstrip().rstrip(";")
                    buffer = []
                    try:
                        if driver == "adbc":
                            execute_adbc(conn, sql)
                        else:
                            execute_pyarrow(conn, sql)
                    except Exception as e:
                        print(f"Error: {e}")
                    print()
            except (EOFError, KeyboardInterrupt):
                print("\nBye!")
                break

    if driver == "adbc":
        conn.close()


if __name__ == "__main__":
    main()
