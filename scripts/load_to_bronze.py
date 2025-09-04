# Ingest raw files into Bronze (Parquet + Delta), with schema validation, partitioning,
# rejects, and manifest tracking in DuckDB.
# Usage: python scripts/load_to_bronze.py --raw data_raw --lake lake --manifest duckdb/warehouse.duckdb
import argparse
import pathlib
import os
import hashlib
import json
import datetime as dt
from datetime import datetime
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.dataset as pads
import pyarrow.parquet as pq
import pyarrow.json as pajson
import pandas as pd
from deltalake import write_deltalake, DeltaTable
from openpyxl import load_workbook
import sys
from decimal import Decimal, ROUND_HALF_UP

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from schemas.schemas import *

# ---------- Manifest Helpers ----------
def init_manifest(conn):
    conn.execute('''
        CREATE TABLE IF NOT EXISTS manifest_processed_files (
            src_path TEXT PRIMARY KEY,
            processed_at TIMESTAMP,
            row_count BIGINT,
            reject_count BIGINT,
            status TEXT
        )
    ''')


def already_processed(conn, p):
    return conn.execute("SELECT 1 FROM manifest_processed_files WHERE src_path = ?", [str(p)]).fetchone() is not None

def mark_processed(conn, p, row_count, reject_count=0, status="SUCCESS"):
    conn.execute("""
        INSERT OR REPLACE INTO manifest_processed_files 
        VALUES (?, ?, ?, ?, ?)
    """, [str(p), dt.datetime.utcnow(), row_count, reject_count, status])


# ---------- Data Writing Helpers ----------
def add_audit_columns(tbl: pa.Table, src_path: pathlib.Path) -> pa.Table:
    now = pa.scalar(dt.datetime.utcnow(), type=pa.timestamp('us'))
    filename = pa.scalar(src_path.name, type=pa.string())

    row_hashes = [
        hashlib.sha256(json.dumps(row, default=str).encode('utf-8')).hexdigest()
        for row in tbl.to_pylist()
    ]

    return (
        tbl
        .append_column('ingestion_ts', pa.array([now.as_py()] * len(tbl), type=pa.timestamp('us')))
        .append_column('src_filename', pa.array([filename.as_py()] * len(tbl), type=pa.string()))
        .append_column('src_row_hash', pa.array(row_hashes, type=pa.string()))
    )

def validate_and_split(tbl: pa.Table, schema: pa.Schema, src_path: pathlib.Path, rejects_dir: pathlib.Path) -> pa.Table:
    try:
        # Try casting the whole table at once
        return tbl.cast(schema, safe=False)
    except pa.ArrowInvalid:
        # If table-level cast fails, do row-by-row validation
        valid_rows = []
        invalid_rows = []

        for row in tbl.to_pylist():
            try:
                pa.Table.from_pylist([row], schema=schema)
                valid_rows.append(row)
            except Exception as e:
                row_with_reason = {**row, "reject_reason": str(e)}
                invalid_rows.append(row_with_reason)

        # Write rejects if any
        if invalid_rows:
            rejects_path = rejects_dir / f"{src_path.stem}_rejects.csv"
            rejects_path.parent.mkdir(parents=True, exist_ok=True)
            with open(rejects_path, 'w', encoding='utf-8') as f:
                f.write(','.join(list(schema.names) + ['reject_reason']) + '\n')
                for r in invalid_rows:
                    f.write(','.join([str(r.get(col, "")) for col in schema.names] + [r['reject_reason']]) + '\n')

        if valid_rows:
            return pa.Table.from_pylist(valid_rows, schema=schema)
        else:
            empty_arrays = [pa.array([], type=field.type) for field in schema]
            return pa.Table.from_arrays(empty_arrays, schema=schema)

def coerce_types(tbl: pa.Table, schema: pa.Schema) -> pa.Table:
    """Force columns to match schema types exactly before validation."""
    for field in schema:
        col_name = field.name
        if col_name not in tbl.column_names:
            continue  # Skip if column missing

        # Boolean columns
        if pa.types.is_boolean(field.type):
            bool_array = pa.array(
                [(str(v).lower() in ['true', '1']) if v not in (None, "") else None
                 for v in tbl.column(col_name).to_pylist()],
                type=pa.bool_()
            )
            tbl = tbl.set_column(tbl.schema.get_field_index(col_name), col_name, bool_array)

        # Date columns
        elif pa.types.is_date32(field.type):
            coerced_values = []
            for v in tbl.column(col_name).to_pylist():
                if v in (None, ""):
                    coerced_values.append(None)
                else:
                    try:
                        if isinstance(v, dt.date):  # already a date
                            coerced_values.append(v)
                        elif isinstance(v, dt.datetime):  # datetime → date
                            coerced_values.append(v.date())
                        elif isinstance(v, (int, float)):  
                            # Excel serial date → datetime
                            coerced_values.append((dt.date(1899, 12, 30) + dt.timedelta(days=int(v))))
                        elif isinstance(v, str):
                            coerced_values.append(pd.to_datetime(v, errors='coerce').date())
                        else:
                            coerced_values.append(None)
                    except Exception:
                        coerced_values.append(None)

            date_array = pa.array(coerced_values, type=pa.date32())
            tbl = tbl.set_column(tbl.schema.get_field_index(col_name), col_name, date_array)


        # Timestamp columns
        elif pa.types.is_timestamp(field.type):
            ts_array = pa.array(
                [None if v in (None, "") else pa.scalar(v, type=pa.timestamp('us')).as_py()
                 for v in tbl.column(col_name).to_pylist()],
                type=pa.timestamp('us')
            )
            tbl = tbl.set_column(tbl.schema.get_field_index(col_name), col_name, ts_array)

        # Decimal columns
        elif pa.types.is_decimal(field.type):
            precision = field.type.precision
            scale = field.type.scale
            coerced_values = []
            for v in tbl.column(col_name).to_pylist():
                if v in (None, ""):
                    coerced_values.append(None)
                    continue
                try:
                    if col_name in ("tax_pct", "line_discount_pct"):
                        v = Decimal(str(v)) / Decimal("100")

                    d = Decimal(str(v)).quantize(Decimal(f"1.{'0'*scale}"), rounding=ROUND_HALF_UP)
                    digits = len(d.as_tuple().digits)

                    if digits > precision:
                        coerced_values.append(str(v))  # invalid precision → keep as string
                    else:
                        coerced_values.append(str(d))  # valid → also string
                except Exception:
                    coerced_values.append(str(v))  # invalid format → string

            # Store as string so casting later will fail for invalids
            dec_array = pa.array(coerced_values, type=pa.string())
            tbl = tbl.set_column(tbl.schema.get_field_index(col_name), col_name, dec_array)


        # String columns
        elif pa.types.is_string(field.type):
            str_array = pa.array(
                ["" if v is None else str(v) for v in tbl.column(col_name).to_pylist()],
                type=pa.string()
            )
            tbl = tbl.set_column(tbl.schema.get_field_index(col_name), col_name, str_array)

    return tbl


def load_table(table_name, schema, raw_root, lake_root, conn, partitioning=None, file_format='csv', mode='full'):
    src_file = raw_root / f"{table_name}.{file_format}"
    src_dir = raw_root / table_name

    # Determine manifest key
    manifest_key = src_dir if src_dir.exists() else src_file

    if mode == "full":
        if already_processed(conn, manifest_key):
            print(f"⏩ Skipping already processed table: {table_name}")
            return
    elif mode == "incremental" and partitioning:
        # We'll check per partition later (like sensors)
        pass

    # ---------- Read source ----------
    if src_file.exists():
        if file_format == 'csv':
            tbl = pacsv.read_csv(src_file)
        elif file_format == 'parquet':
            tbl = pq.read_table(src_file)
        elif file_format == 'jsonl':
            tbl = pajson.read_json(src_file)
        elif file_format == 'xlsx':
            wb = load_workbook(src_file)
            ws = wb.active
            data = list(ws.values)
            header, rows = data[0], data[1:]
            df = pd.DataFrame(rows, columns=header)
            tbl = pa.Table.from_pandas(df)

        elif file_format == 'delta':
            dtbl = DeltaTable(str(src_file))
            tbl = dtbl.to_pyarrow_table()
        else:
            raise ValueError(f"Unsupported format: {file_format}")
    elif src_dir.exists() and src_dir.is_dir():
        if file_format == 'csv':
            dataset = pads.dataset(str(src_dir), format='csv', partitioning='hive')
            tbl = dataset.to_table()
        else:
            raise ValueError(f"Partitioned reading implemented only for CSV in this loader")
    else:
        print(f"⚠️ Skipping {table_name}: No source file or folder found")
        return

    # ---------- Drop extra cols before validation ----------
    drop_cols = set(tbl.column_names) - set(schema.names) 
    if drop_cols:
        print(f"ℹ️ Dropping columns before validation for {table_name}: {drop_cols}")
        tbl = tbl.drop(list(drop_cols))

    # ---------- Coerce types and validate ----------
    tbl = coerce_types(tbl, schema)
    tbl = validate_and_split(tbl, schema, src_dir if src_dir.exists() else src_file, lake_root / '_rejects')

    # ---------- Add audit columns ----------
    tbl = add_audit_columns(tbl, src_dir if src_dir.exists() else src_file)


    # ---------- Write to Parquet ----------
    pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name

    if partitioning:
        # Partitioned tables (fact/event/IoT) → incremental safe
        partition_schema = pa.schema([pa.field(col, tbl.schema.field(col).type) for col in partitioning])
        pads.write_dataset(
            tbl,
            base_dir=str(pq_base),
            format='parquet',
            partitioning=pads.partitioning(flavor="hive", schema=partition_schema),
            existing_data_behavior='overwrite_or_ignore'  # ✅ Incremental safe
        )
    else:
        # Small unpartitioned tables (dimensions, exchange_rates) → full refresh
        pads.write_dataset(
            tbl,
            base_dir=str(pq_base),
            format='parquet',
            existing_data_behavior='delete_matching'  # ✅ Always replace
        )

    # ---------- Write to Delta ----------
    dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
    if partitioning:
        write_deltalake(str(dl_base), tbl, mode='append', partition_by=partitioning or [])
    else:
        write_deltalake(str(dl_base), tbl, mode='overwrite')  # Full refresh for small tables


        # ---------- Mark processed ----------
        mark_processed(conn, src_dir if src_dir.exists() else src_file, len(tbl))
        print(f"✅ Bronze load completed for {table_name} ({len(tbl)} rows)")

def load_orders_header(raw_root, lake_root, conn):
    table_name = "orders_header"
    schema = orders_header_schema
    src_dir = raw_root / table_name

    if not src_dir.exists():
        print(f"⚠️ Skipping {table_name}: No source folder found")
        return

    # Loop through each partition folder (order_dt_local=YYYY-MM-DD)
    for partition_dir in src_dir.iterdir():
        if not partition_dir.is_dir():
            continue

        # Idempotency check — skip if already processed
        if already_processed(conn, partition_dir):
            print(f"⏩ Skipping already processed partition: {partition_dir}")
            continue

        # ---------- Read partition ----------
        dataset = pads.dataset(str(partition_dir), format='csv', partitioning='hive')
        tbl = dataset.to_table()

        # ---------- Drop extra columns ----------
        drop_cols = set(tbl.column_names) - set(schema.names)
        if drop_cols:
            print(f"ℹ️ Dropping columns before validation for {table_name}: {drop_cols}")
            tbl = tbl.drop(list(drop_cols))

        # ---------- Coerce types and validate ----------
        tbl = coerce_types(tbl, schema)
        tbl = validate_and_split(tbl, schema, partition_dir, lake_root / '_rejects')

        # ---------- Add audit columns ----------
        tbl = add_audit_columns(tbl, partition_dir)

        # ---------- Write to Parquet ----------
        pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
        partition_schema = pa.schema([pa.field('order_dt_local', tbl.schema.field('order_dt_local').type)])
        pads.write_dataset(
            tbl,
            base_dir=str(pq_base),
            format='parquet',
            partitioning=pads.partitioning(flavor="hive", schema=partition_schema),
            existing_data_behavior='overwrite_or_ignore'
        )

        # ---------- Write to Delta ----------
        dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
        write_deltalake(str(dl_base), tbl, mode='append', partition_by=['order_dt_local'])

        # ---------- Mark partition as processed ----------
        mark_processed(conn, partition_dir, len(tbl))
        print(f"✅ Bronze load completed for {table_name} partition {partition_dir} ({len(tbl)} rows)")



def load_orders_lines(raw_root, lake_root, conn):
    table_name = "orders_lines"
    schema = orders_lines_schema
    src_dir = raw_root / table_name

    if not src_dir.exists():
        print(f"⚠️ Skipping {table_name}: No source folder found")
        return

    for partition_dir in src_dir.iterdir():
        if not partition_dir.is_dir():
            continue

        if already_processed(conn, partition_dir):
            print(f"⏩ Skipping already processed partition: {partition_dir}")
            continue

        dataset = pads.dataset(str(partition_dir), format='csv', partitioning='hive')
        tbl = dataset.to_table()

        extra_cols = set(tbl.column_names) - set(schema.names)
        if extra_cols:
            print(f"ℹ️ Dropping extra columns for {table_name}: {extra_cols}")
            tbl = tbl.drop(list(extra_cols))

        tbl = coerce_types(tbl, schema)
        tbl = validate_and_split(tbl, schema, partition_dir, lake_root / '_rejects')

        # Load orders_header Bronze for order_dt_local
        orders_header_path = lake_root / 'bronze' / 'parquet' / 'samples' / 'orders_header'
        orders_header_tbl = pads.dataset(
            str(orders_header_path),
            format='parquet',
            partitioning='hive'
        ).to_table(columns=['order_id', 'order_dt_local'])

        joined = tbl.join(orders_header_tbl, keys=['order_id'], join_type='left outer')

        # FK violation check
        valid_mask = pa.compute.invert(pa.compute.is_null(joined['order_dt_local']))
        valid_tbl = joined.filter(valid_mask)
        rejects_tbl = joined.filter(pa.compute.is_null(joined['order_dt_local']))

        if len(rejects_tbl) > 0:
            print(f"⚠️ Found {len(rejects_tbl)} FK violations in {table_name}")
            rejects_tbl = rejects_tbl.append_column(
                'reject_reason',
                pa.array(["FK violation: order_id not found in orders_header"] * len(rejects_tbl))
            )
            pq.write_table(rejects_tbl, str(lake_root / '_rejects' / f"{table_name}_rejects.parquet"))

        valid_tbl = add_audit_columns(valid_tbl, partition_dir)

        pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
        partition_schema = pa.schema([pa.field('order_dt_local', valid_tbl.schema.field('order_dt_local').type)])
        pads.write_dataset(
            valid_tbl,
            base_dir=str(pq_base),
            format='parquet',
            partitioning=pads.partitioning(flavor="hive", schema=partition_schema),
            existing_data_behavior='overwrite_or_ignore'
        )

        dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
        write_deltalake(str(dl_base), valid_tbl, mode='append', partition_by=['order_dt_local'])

        mark_processed(conn, partition_dir, len(valid_tbl))
        print(f"✅ Bronze load completed for {table_name} partition {partition_dir} ({len(valid_tbl)} rows)")

from datetime import datetime

def load_events(raw_root, lake_root, conn):
    table_name = "events"
    schema = events_schema
    src_dir = raw_root / table_name

    if not src_dir.exists():
        print(f"⚠️ Skipping {table_name}: No source folder found")
        return

    for partition_dir in src_dir.iterdir():
        if not partition_dir.is_dir():
            continue

        if already_processed(conn, partition_dir):
            print(f"⏩ Skipping already processed partition: {partition_dir}")
            continue

        valid_rows = []
        invalid_rows = []

        for jsonl_file in partition_dir.rglob("*.jsonl"):
            with open(jsonl_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                        ts_str = obj.get("event_ts")
                        if ts_str:
                            event_dt_val = datetime.fromisoformat(ts_str).date()
                        else:
                            event_dt_val = None
                        valid_rows.append({
                            "json": line,
                            "event_dt": event_dt_val
                        })
                    except Exception as e:
                        invalid_rows.append({
                            "json": line,
                            "reject_reason": str(e)
                        })

        if invalid_rows:
            rejects_path = lake_root / '_rejects' / f"{table_name}_malformed_json.csv"
            rejects_path.parent.mkdir(parents=True, exist_ok=True)
            with open(rejects_path, 'a', encoding='utf-8') as f:
                for r in invalid_rows:
                    f.write(f"{r['json']},{r['reject_reason']}\n")
            print(f"⚠️ {len(invalid_rows)} malformed JSON lines written to {rejects_path}")

        if valid_rows:
            tbl = pa.Table.from_pylist(valid_rows, schema=pa.schema([
                pa.field("json", pa.string()),
                pa.field("event_dt", pa.date32())
            ]))

            missing_mask = pa.compute.is_null(tbl['event_dt'])
            missing_tbl = tbl.filter(missing_mask)
            tbl = tbl.filter(pa.compute.invert(missing_mask))

            if len(missing_tbl) > 0:
                missing_path = lake_root / '_rejects' / f"{table_name}_missing_event_dt.parquet"
                pq.write_table(missing_tbl, str(missing_path))
                print(f"⚠️ {len(missing_tbl)} rows missing event_dt written to {missing_path}")

            if len(tbl) > 0:
                tbl = add_audit_columns(tbl, partition_dir)

                pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
                partition_schema = pa.schema([pa.field('event_dt', pa.date32())])
                pads.write_dataset(
                    tbl,
                    base_dir=str(pq_base),
                    format='parquet',
                    partitioning=pads.partitioning(flavor="hive", schema=partition_schema),
                    existing_data_behavior='overwrite_or_ignore'
                )

                dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
                write_deltalake(str(dl_base), tbl, mode='append', partition_by=['event_dt'])

                mark_processed(conn, partition_dir, len(tbl))
                print(f"✅ Bronze load completed for {table_name} partition {partition_dir} ({len(tbl)} valid rows)")


def load_sensors(raw_root, lake_root, conn):
    table_name = "sensors"
    schema = sensors_schema
    src_dir = raw_root / table_name

    if not src_dir.exists():
        print(f"⚠️ Skipping {table_name}: No source folder found")
        return

    for partition_path in src_dir.rglob("*.csv"):
        partition_dir = partition_path.parent
        if already_processed(conn, partition_dir):
            print(f"⏩ Skipping already processed partition: {partition_dir}")
            continue

        dataset = pads.dataset(str(partition_dir), format='csv', partitioning='hive')
        tbl = dataset.to_table()

        extra_cols = set(tbl.column_names) - set(schema.names) - {'store_id', 'month'}
        if extra_cols:
            tbl = tbl.drop(list(extra_cols))

        tbl = coerce_types(tbl, schema)
        tbl = validate_and_split(tbl, schema, partition_dir, lake_root / '_rejects')

        # Reject missing sensor_ts
        missing_ts_mask = pa.compute.is_null(tbl['sensor_ts'])
        missing_ts_tbl = tbl.filter(missing_ts_mask)
        tbl = tbl.filter(pa.compute.invert(missing_ts_mask))

        reject_count = len(missing_ts_tbl)
        if reject_count > 0:
            pq.write_table(missing_ts_tbl, str(lake_root / '_rejects' / f"{table_name}_missing_sensor_ts.parquet"))

        # Reject out-of-range temperature/humidity
        temp_col = tbl['temperature_c']
        hum_col = tbl['humidity_pct']

        temp_out_mask = pa.compute.or_(
            pa.compute.less(temp_col, pa.scalar(Decimal("-50.00"), type=pa.decimal128(5, 2))),
            pa.compute.greater(temp_col, pa.scalar(Decimal("50.00"), type=pa.decimal128(5, 2)))
        )

        hum_out_mask = pa.compute.or_(
            pa.compute.less(hum_col, pa.scalar(Decimal("0.00"), type=pa.decimal128(5, 2))),
            pa.compute.greater(hum_col, pa.scalar(Decimal("100.00"), type=pa.decimal128(5, 2)))
        )

        out_of_range_mask = pa.compute.or_(temp_out_mask, hum_out_mask)
        out_of_range_tbl = tbl.filter(out_of_range_mask)
        tbl = tbl.filter(pa.compute.invert(out_of_range_mask))

        reject_count += len(out_of_range_tbl)
        if len(out_of_range_tbl) > 0:
            pq.write_table(out_of_range_tbl, str(lake_root / '_rejects' / f"{table_name}_out_of_range.parquet"))

        # Add audit columns
        tbl = add_audit_columns(tbl, partition_dir)

        # TEMP derive month for partitioning
        month_vals = [
            f"{v.year}-{str(v.month).zfill(2)}" if v is not None else None
            for v in tbl.column('sensor_ts').to_pylist()
        ]
        tbl_with_month = tbl.append_column('month', pa.array(month_vals, type=pa.string()))

        # Write to Parquet with Hive-style partitioning
        pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
        partition_schema = pa.schema([
            pa.field('store_id', pa.int64()),
            pa.field('month', pa.string())
        ])
        pads.write_dataset(
            tbl_with_month,
            base_dir=str(pq_base),
            format='parquet',
            partitioning=pads.partitioning(flavor="hive", schema=partition_schema),
            existing_data_behavior='overwrite_or_ignore'
        )

        # Write to Delta
        dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
        write_deltalake(str(dl_base), tbl_with_month, mode='append', partition_by=['store_id', 'month'])

        # Mark processed
        mark_processed(conn, partition_dir, len(tbl), reject_count, status="SUCCESS")
        print(f"✅ Bronze load completed for {table_name} partition {partition_dir} ({len(tbl)} valid rows, {reject_count} rejects)")

import glob

def load_shipments(raw_root, lake_root, conn):
    table_name = "shipments"
    schema = shipments_schema

    # Search for file starting with "shipments" and ending with ".parquet"
    matching_files = list(raw_root.glob("shipments*.parquet"))
    if not matching_files:
        print(f"⚠️ Skipping {table_name}: No matching file found")
        return

    src_file = matching_files[0]  # Take first match
    print(f"ℹ️ Found shipments file: {src_file}")

    # Idempotency check (full refresh, skip if already processed)
    if already_processed(conn, src_file):
        print(f"⏩ Skipping already processed table: {table_name}")
        return

    # Read Parquet
    tbl = pq.read_table(src_file)

    # Drop extra columns before validation
    drop_cols = set(tbl.column_names) - set(schema.names)
    if drop_cols:
        print(f"ℹ️ Dropping columns before validation for {table_name}: {drop_cols}")
        tbl = tbl.drop(list(drop_cols))

    # Coerce types and validate
    tbl = coerce_types(tbl, schema)
    tbl = validate_and_split(tbl, schema, src_file, lake_root / '_rejects')

    # Add audit columns
    tbl = add_audit_columns(tbl, src_file)

    # Write to Parquet (full refresh)
    pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
    pads.write_dataset(
        tbl,
        base_dir=str(pq_base),
        format='parquet',
        existing_data_behavior='delete_matching'  # PyArrow 17+ full refresh
    )

    # Write to Delta (full refresh)
    dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name
    write_deltalake(str(dl_base), tbl, mode='overwrite')

    # Mark processed
    mark_processed(conn, src_file, len(tbl))
    print(f"✅ Bronze load completed for {table_name} ({len(tbl)} rows)")


def load_returns(raw_root, lake_root, conn):
    table_name = "returns"
    base_schema = returns_day1_schema  # canonical schema
    src_dir = raw_root / table_name    # Delta source

    if not src_dir.exists():
        print(f"⚠️ Skipping {table_name}: No source folder found")
        return

    if already_processed(conn, src_dir):
        print(f"⏩ Skipping already processed table: {table_name}")
        return

    # ---------- Read from Delta source ----------
    dtbl = DeltaTable(str(src_dir))
    tbl = dtbl.to_pyarrow_table()

    # ---------- Detect new columns ----------
    existing_cols = set(base_schema.names)
    raw_cols = set(tbl.column_names)
    new_cols = raw_cols - existing_cols

    if new_cols:
        print(f"ℹ️ New columns detected in raw data: {new_cols}")

        # Append new columns to schema dynamically (as string type for safety)
        for col in new_cols:
            base_schema = base_schema.append(pa.field(col, pa.string()))

    # ---------- Coerce types for known columns only ----------
    tbl = coerce_types(tbl, base_schema)

    # ---------- Validate known columns ----------
    # (Skip validate_and_split for new columns to avoid casting errors)
    known_cols_tbl = validate_and_split(tbl.select(base_schema.names), base_schema, src_dir, lake_root / '_rejects')

    # ---------- Add audit columns ----------
    final_tbl = add_audit_columns(known_cols_tbl, src_dir)

    # ---------- Bronze Delta destination ----------
    dl_base = lake_root / 'bronze' / 'delta'/ 'samples' / table_name

    if DeltaTable.is_deltatable(str(dl_base)):
        print(f"🔄 Performing UPSERT for {table_name} with schema evolution...")
        existing_dt = DeltaTable(str(dl_base))
        existing_dt.merge(
            source=final_tbl.to_pandas(),
            predicate="s.return_id = t.return_id",
            source_alias="s",
            target_alias="t"
        ).when_matched_update_all() \
         .when_not_matched_insert_all() \
         .execute()
    else:
        print(f"🆕 Creating new Delta table for {table_name} with schema evolution...")
        # This will now include new columns automatically
        write_deltalake(str(dl_base), final_tbl, mode='overwrite')

    # ---------- Also write to Parquet ----------
    pq_base = lake_root / 'bronze' / 'parquet'/ 'samples' / table_name
    pads.write_dataset(
        final_tbl,
        base_dir=str(pq_base),
        format='parquet',
        existing_data_behavior='delete_matching'
    )

    # ---------- Mark processed ----------
    mark_processed(conn, src_dir, len(final_tbl))
    print(f"✅ Bronze load completed for {table_name} ({len(final_tbl)} rows)")


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=str, default='data_raw')
    ap.add_argument('--lake', type=str, default='lake')
    ap.add_argument('--manifest', type=str, default='duckdb/warehouse.duckdb')
    return ap.parse_args()

def ensure_dirs(lake_root):
    for sub in ['bronze/parquet', 'bronze/delta']:
        (lake_root / sub).mkdir(parents=True, exist_ok=True)
    (lake_root / '_rejects').mkdir(parents=True, exist_ok=True)

def main():
    args = parse_args()
    raw_root = pathlib.Path(args.raw)
    lake_root = pathlib.Path(args.lake)
    ensure_dirs(lake_root)
    pathlib.Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(args.manifest)
    conn.execute("INSTALL delta; LOAD delta;")
    init_manifest(conn)

    # Full refresh tables
    load_table('customers', customers_schema, raw_root, lake_root, conn, mode="full")
    load_table('products', products_schema, raw_root, lake_root, conn, mode="full")
    load_table('stores', stores_schema, raw_root, lake_root, conn, mode="full")
    load_table('suppliers', suppliers_schema, raw_root, lake_root, conn, mode="full")
    load_table('exchange_rates', exchange_rates_schema, raw_root, lake_root, conn, file_format='xlsx', mode="full")
    

    # Incremental tables
    load_orders_header(raw_root, lake_root, conn)
    load_orders_lines(raw_root, lake_root, conn)  # Special incremental loader
    load_events(raw_root, lake_root, conn)        # Incremental by date
    load_sensors(raw_root, lake_root, conn)       # Incremental by store_id/month

    load_shipments(raw_root, lake_root, conn)
    # Special UPSERT
    load_returns(raw_root, lake_root, conn)


if __name__ == '__main__':
    main()
