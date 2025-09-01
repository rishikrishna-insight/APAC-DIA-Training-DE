# Generate synthetic raw data locally with controlled edge cases.
# Usage: python scripts/generate_data.py --seed 42 --out data_raw
import argparse
import pathlib
import random
import string
from datetime import datetime, timedelta, date
import numpy as np
import pandas as pd
from faker import Faker
import sys, os
import json
import csv
from decimal import Decimal, ROUND_HALF_UP
from openpyxl import Workbook
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake, DeltaTable
from concurrent.futures import ProcessPoolExecutor

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import schemas
from schemas.schemas import (
    customers_schema,
    products_schema,
    stores_schema,
    suppliers_schema,
    orders_header_schema,
    orders_lines_schema,
    events_schema,
    sensors_schema,
    exchange_rates_schema,
    shipments_schema,
    returns_day1_schema
)

# ---------- Helpers ----------
def ensure_dir(path):
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def rand_date(start: date, end: date):
    """Random date between start and end."""
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_datetime(start: datetime, end: datetime):
    """Random datetime between start and end."""
    return start + timedelta(
        days=random.randint(0, (end - start).days),
        seconds=random.randint(0, 86399)
    )

def rand_key(prefix: str, length: int):
    """Generate random key like CUST-XXXXXXXX."""
    return prefix + "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

# ---------- Dataset Generators ----------
def generate_customers(path: pathlib.Path, scale: float, seed: int):
    random.seed(seed)
    np.random.seed(seed)
    fake = Faker("en_AU")

    target_rows = int(80000 * scale)
    generated_keys = []

    with path.open("w", encoding="utf-8") as f:
        # Header from schema
        f.write(",".join(customers_schema.names) + "\n")

        for i in range(1, target_rows + 1):
            # natural_key with 0.2% duplicates
            if generated_keys and random.random() < 0.002:
                natural_key = random.choice(generated_keys)
            else:
                natural_key = rand_key("CUST-", 8)
                generated_keys.append(natural_key)

            # email anomaly (~0.9% bad)
            email = fake.email() if random.random() > 0.009 else "bad_email"

            # phone & address anomalies (~7% nulls)
            phone = fake.phone_number().replace(",", " ") if random.random() > 0.07 else ""
            address_line1 = fake.street_address().replace(",", " ") if random.random() > 0.07 else ""

            # address_line2 (50% nulls)
            address_line2 = "" if random.random() < 0.5 else fake.secondary_address().replace(",", " ")

            # location
            city = fake.city().replace(",", " ")
            state_region = fake.state_abbr()
            postcode = str(fake.postcode()).zfill(4)
            country_code = "AU"
            latitude = round(random.uniform(-44.0, -10.0), 6)
            longitude = round(random.uniform(112.0, 154.0), 6)

            # birth_date — between 1960-01-01 and 2005-12-31
            birth_date = rand_date(date(1960, 1, 1), date(2005, 12, 31))

                        # join_ts — between 2020-01-01 and now
            join_ts = rand_datetime(datetime(2020, 1, 1), datetime.now())

            # boolean flags
            is_vip = random.random() < 0.15        # ~15% True
            gdpr_consent = random.random() > 0.05  # ~95% True

            # Write row in schema order
            row = [
                i,
                natural_key,
                fake.first_name(),
                fake.last_name(),
                email,
                phone,
                address_line1,
                address_line2,
                city,
                state_region,
                postcode,
                country_code,
                latitude,
                longitude,
                birth_date.isoformat(),
                join_ts.isoformat(),
                str(is_vip),
                str(gdpr_consent)
            ]
            f.write(",".join(map(str, row)) + "\n")

    print(f"✅ Generated {target_rows} customers → {path}")


def generate_products(path: pathlib.Path, scale: float, seed: int):

    random.seed(seed)
    np.random.seed(seed)
    fake = Faker("en_AU")

    target_rows = int(25000 * scale)
    generated_skus = []
    end_date = date(2025, 8, 29)

    with path.open("w", encoding="utf-8") as f:
        # Header from schema
        f.write(",".join(products_schema.names) + "\n")

        for i in range(1, target_rows + 1):
            # SKU with 0.1% duplicates
            if generated_skus and random.random() < 0.001:
                sku = random.choice(generated_skus)
            else:
                sku = rand_key("SKU-", 6)
                generated_skus.append(sku)

            # Product name & category
            name = fake.word().capitalize() + " " + fake.word().capitalize()
            category = random.choice(["Electronics", "Clothing", "Home", "Sports", "Books"])
            subcategory = fake.word().capitalize()

            # Price with 0.1–0.5% invalid/missing
            if random.random() < 0.003:
                current_price = ""  # invalid/missing
            else:
                current_price = Decimal(random.uniform(1.0, 99999.99)).quantize(
                    Decimal("0.0001"), rounding=ROUND_HALF_UP
                )

            currency = "AUD"

            # Lifecycle dates
            introduced_dt = rand_date(date(2000, 1, 1), date(2024, 12, 31))

            if random.random() < 0.2:  # 20% discontinued products
                is_discontinued = True

                if random.random() < 0.8:  # 80% of discontinued have valid date
                    # Earliest possible discontinued date = 1 year after introduction
                    try:
                        earliest_disc = introduced_dt.replace(year=introduced_dt.year + 1)
                    except ValueError:
                        # Handle Feb 29 leap year issue
                        earliest_disc = introduced_dt + timedelta(days=365)

                    # Ensure earliest_disc is not after cutoff
                    if earliest_disc > end_date:
                        discontinued_dt = end_date
                    else:
                        discontinued_dt = rand_date(earliest_disc, end_date)
                else:
                    # 20% of discontinued have missing date
                    discontinued_dt = ""
            else:
                is_discontinued = False
                discontinued_dt = ""

            # Write row in schema order
            row = [
                i,
                sku,
                name,
                category,
                subcategory,
                current_price,
                currency,
                str(is_discontinued),
                introduced_dt.isoformat(),
                discontinued_dt if discontinued_dt == "" else discontinued_dt.isoformat()
            ]
            f.write(",".join(map(str, row)) + "\n")

    print(f"✅ Generated {target_rows} products → {path}")

def generate_stores(path: pathlib.Path, scale: float, seed: int):
    random.seed(seed)
    np.random.seed(seed)
    fake = Faker("en_AU")

    target_rows = int(5000 * scale)
    generated_codes = []

    with path.open("w", encoding="utf-8") as f:
        f.write(",".join(stores_schema.names) + "\n")

        for i in range(1, target_rows + 1):
            # store_code with occasional duplicates (0.5%)
            if generated_codes and random.random() < 0.005:
                store_code = random.choice(generated_codes)
            else:
                store_code = rand_key("STORE-", 5)
                generated_codes.append(store_code)

            # Clean text fields to avoid commas breaking CSV
            name = fake.company().replace(",", " ")
            channel = random.choice(["web", "pos"])
            region = random.choice(["North", "South", "East", "West"])
            state = fake.state_abbr()

            # Latitude/Longitude with some impossible values (0.5%)
            if random.random() < 0.005:
                latitude = round(random.uniform(-200.0, 200.0), 6)  # impossible values
                longitude = round(random.uniform(-200.0, 200.0), 6)
            else:
                latitude = round(random.uniform(-44.0, -10.0), 6)
                longitude = round(random.uniform(112.0, 154.0), 6)

            open_dt = rand_date(date(1990, 1, 1), date(2025, 1, 1))

            if random.random() < 0.2:  # 20% closed stores
                close_dt = rand_date(open_dt, date(2025, 8, 29))
            else:
                close_dt = ""

            # Row matches stores_schema.names order exactly
            row = [
                i,
                store_code,
                name,
                channel,
                region,
                state,
                latitude,
                longitude,
                open_dt.isoformat(),
                close_dt if close_dt == "" else close_dt.isoformat()
            ]
            f.write(",".join(map(str, row)) + "\n")

    print(f"✅ Generated {target_rows} stores → {path}")


def generate_suppliers(path: pathlib.Path, scale: float, seed: int):
    random.seed(seed)
    np.random.seed(seed)
    fake = Faker("en_AU")

    target_rows = int(8000 * scale)

    with path.open("w", encoding="utf-8") as f:
        # Write header from schema
        f.write(",".join(suppliers_schema.names) + "\n")

        for i in range(1, target_rows + 1):
            supplier_code = rand_key("SUP-", 6)
            name = fake.company().replace(",", " ")
            country_code = random.choice(["AU", "NZ", "US", "CN", "IN", "GB"])
            lead_time_days = random.randint(1, 90)
            preferred = random.random() < 0.3  # 30% preferred suppliers

            # Row matches schema order exactly
            row = [
                i,
                supplier_code,
                name,
                country_code,
                lead_time_days,
                str(preferred)
            ]
            f.write(",".join(map(str, row)) + "\n")

    print(f"✅ Generated {target_rows} suppliers → {path}")


def _generate_orders_header_for_date(params):
    part_date, rows_per_date, start_order_id, scale, seed, out_dir = params

    # Safe seed
    safe_seed = (seed + int(part_date.strftime("%Y%m%d"))) % (2**32)
    random.seed(safe_seed)
    np.random.seed(safe_seed)

    fake = Faker("en_AU")
    partition_dir = out_dir / "orders_header" / f"order_dt={part_date}"
    ensure_dir(partition_dir)
    file_path = partition_dir / "part-0001.csv"

    max_customer_id = max(1, int(80000 * scale))
    max_store_id = max(1, int(5000 * scale))
    payment_methods = ["credit_card", "paypal", "bank_transfer", "gift_card", "cash"]

    with file_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(orders_header_schema.names)

        order_id = start_order_id
        for _ in range(rows_per_date):
            order_ts = fake.date_time_between_dates(
                datetime_start=datetime.combine(part_date, datetime.min.time()),
                datetime_end=datetime.combine(part_date, datetime.max.time())
            ).isoformat()
            order_dt_local = part_date
            customer_id = random.randint(1, max_customer_id) if random.random() > 0.01 else random.randint(max_customer_id+1, max_customer_id+1000)
            store_id = random.randint(1, max_store_id) if random.random() > 0.01 else random.randint(max_store_id+1, max_store_id+500)
            channel = "web" if random.random() < 0.6 else "pos"
            payment_method = np.random.choice(payment_methods, p=[0.5, 0.2, 0.1, 0.1, 0.1])
            coupon_code = "" if random.random() > 0.15 else "SAVE-" + rand_key("", 4)
            r_fee = random.random()
            if r_fee < 0.20:
                shipping_fee = Decimal("0.00")
            elif r_fee < 0.70:
                shipping_fee = Decimal(random.uniform(5.00, 15.00)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            elif r_fee < 0.95:
                shipping_fee = Decimal(random.uniform(15.01, 30.00)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            else:
                shipping_fee = Decimal(random.uniform(30.01, 50.00)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            writer.writerow([order_id, order_ts, order_dt_local, customer_id, store_id, channel, payment_method, coupon_code, shipping_fee, "AUD"])
            order_id += 1

    return f"✅ Orders_header {part_date} done"


def generate_orders_header(out_dir: pathlib.Path, scale: float, seed: int, workers: int = 4):
    target_rows = int(1_000_000 * scale)
    days_span = max(14, int(1100 * scale))
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=days_span)
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    rows_per_date = max(1, target_rows // len(date_list))

    # Pre-calculate starting order_id for each partition
    params_list = []
    current_id = 1
    for d in date_list:
        params_list.append((d, rows_per_date, current_id, scale, seed, out_dir))
        current_id += rows_per_date

    print(f"▶ Generating {target_rows} orders_header using {workers} workers...")
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for idx, result in enumerate(executor.map(_generate_orders_header_for_date, params_list), start=1):
            if idx % 5 == 0 or idx == len(params_list):
                print(f"Progress: {idx}/{len(params_list)} partitions complete")

    print(f"✅ Generated {target_rows} orders_header → {out_dir/'orders_header'}")


def _generate_orders_lines_for_partition(params):
    part_folder, max_product_id, out_dir, scale, seed = params

    # Safe seed
    safe_seed = (seed + abs(hash(str(part_folder)))) % (2**32)
    random.seed(safe_seed)
    np.random.seed(safe_seed)

    date_str = part_folder.name.split("=")[1]
    target_partition_dir = out_dir / "orders_lines" / f"order_dt={date_str}"
    ensure_dir(target_partition_dir)

    for part_file in sorted(part_folder.glob("part-*.csv")):
        output_file = target_partition_dir / part_file.name
        with part_file.open("r", encoding="utf-8") as infile, output_file.open("w", encoding="utf-8") as outfile:
            reader = csv.DictReader(infile)
            outfile.write(",".join(orders_lines_schema.names) + "\n")

            for row in reader:
                order_id = row["order_id"]
                num_lines = random.randint(1, 5)
                for line_num in range(1, num_lines + 1):
                    product_id = random.randint(1, max_product_id) if random.random() > 0.01 else random.randint(max_product_id+1, max_product_id+500)
                    qty = random.choice([0, -1]) if random.random() < 0.001 else random.randint(1, 5)
                    unit_price = Decimal("0.0000") if random.random() < 0.0005 else Decimal(random.uniform(1.00, 999.99)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    discount_value = random.uniform(50, 100) if random.random() < 0.10 else random.uniform(0, 50)
                    line_discount_pct = Decimal(discount_value).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    tax_pct = Decimal(random.uniform(5, 15)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
                    outfile.write(",".join(map(str, [order_id, line_num, product_id, qty, unit_price, line_discount_pct, tax_pct])) + "\n")



def generate_orders_lines(out_dir: pathlib.Path, scale: float, seed: int, workers: int = 4):
    max_product_id = max(1, int(25000 * scale))
    orders_header_dir = out_dir / "orders_header"
    params_list = [(folder, max_product_id, out_dir, scale, seed) for folder in sorted(orders_header_dir.glob("order_dt=*"))]

    print(f"▶ Generating orders_lines using {workers} workers...")
    with ProcessPoolExecutor(max_workers=workers) as executor:
        result = list(executor.map(_generate_orders_lines_for_partition, params_list))
            
    print(f"✅ Generated orders_lines for {len(result)}→ {out_dir/'orders_lines'}")


def _generate_events_for_date(params):
    event_date, rows_per_day, scale, seed, out_dir = params

    # Safe seed
    safe_seed = (seed + abs(hash(f"event_{event_date}"))) % (2**32)
    random.seed(safe_seed)
    np.random.seed(safe_seed)

    fake = Faker("en_AU")
    partition_dir = out_dir / "events" / f"event_dt={event_date}"
    ensure_dir(partition_dir)
    file_path = partition_dir / f"events_{event_date.strftime('%Y%m%d')}.jsonl"

    event_types = np.array(["page_view", "search", "add_to_cart", "purchase"])
    chosen_types = np.random.choice(event_types, size=rows_per_day)

    with file_path.open("w", encoding="utf-8") as f:
        for i in range(rows_per_day):
            event_type = chosen_types[i]
            envelope = {
                "event_id": f"{event_date.strftime('%Y%m%d')}-{i+1}",
                "event_ts": fake.date_time_between_dates(
                    datetime_start=datetime.combine(event_date, datetime.min.time()),
                    datetime_end=datetime.combine(event_date, datetime.max.time())
                ).isoformat(),
                "event_type": event_type,
                "user_id": random.randint(1, int(80000 * scale)),
                "session_id": fake.uuid4()
            }
            payload = {}
            if event_type == "page_view":
                payload = {"page": random.choice(["/home", "/products", "/cart", "/checkout"])}
            elif event_type == "search":
                payload = {"query": random.choice(["laptop", "shoes", "headphones", "camera"])}
            elif event_type == "add_to_cart":
                payload = {"product_id": random.randint(1, int(25000 * scale)), "qty": random.randint(1, 3)}
            elif event_type == "purchase":
                payload = {"order_id": random.randint(1, int(1_000_000 * scale)), "amount": round(random.uniform(10, 500), 2)}

            record = {**envelope, "payload": payload}

            # Anomalies
            anomaly_chance = random.random()
            if anomaly_chance < 0.0005:
                f.write("{bad_json_line}\n")
                continue
            elif anomaly_chance < 0.002:
                keys_to_remove = random.sample(list(envelope.keys()), k=random.randint(1, 2))
                for key in keys_to_remove:
                    record.pop(key, None)

            f.write(json.dumps(record) + "\n")



def generate_events(out_dir: pathlib.Path, scale: float, seed: int, workers: int = 4):
    target_rows = int(2_000_000 * scale)
    days_span = max(7, int(90 * scale))
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=days_span)
    date_list = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    rows_per_day = max(1, target_rows // len(date_list))

    params_list = [(d, rows_per_day, scale, seed, out_dir) for d in date_list]

    print(f"▶ Generating {target_rows} events using {workers} workers...")
    with ProcessPoolExecutor(max_workers=workers) as executor:
        result = list(executor.map(_generate_events_for_date, params_list))
            
    print(f"✅ Generated {len(result)} events → {out_dir/'events'}")



def _generate_sensors_for_store(params):
    store_id, month_list, rows_per_store_month, max_rows_per_file, scale, seed, out_dir = params

    # Safe seed
    safe_seed = (seed + abs(hash(f"store_{store_id}"))) % (2**32)
    random.seed(safe_seed)
    np.random.seed(safe_seed)

    fake = Faker("en_AU")

    for month in month_list:
        partition_dir = out_dir / "sensors" / f"store_id={store_id}" / f"month={month}"
        ensure_dir(partition_dir)

        file_index = 1
        row_count_in_file = 0
        file_path = partition_dir / f"part-{file_index:04d}.csv"
        f = file_path.open("w", encoding="utf-8", newline="")
        writer = csv.writer(f)
        writer.writerow(sensors_schema.names)

        # Vectorized numeric generation
        year, month_num = map(int, month.split("-"))
        start_date = datetime(year, month_num, 1)
        end_date = datetime(year + (month_num // 12), (month_num % 12) + 1, 1)

        ts_array = [
            fake.date_time_between_dates(start_date, end_date).isoformat() if random.random() > 0.001 else ""
            for _ in range(rows_per_store_month)
        ]
        shelf_ids = [f"SHELF-{random.randint(1, 100)}" for _ in range(rows_per_store_month)]

        temp_anomaly_mask = np.random.rand(rows_per_store_month) < 0.003
        temperatures = np.where(temp_anomaly_mask,
                                 np.random.uniform(1000, 5000, rows_per_store_month),
                                 np.random.uniform(-10, 50, rows_per_store_month)).round(2)

        hum_anomaly_mask = np.random.rand(rows_per_store_month) < 0.003
        humidities = np.where(hum_anomaly_mask,
                              np.random.uniform(1000, 5000, rows_per_store_month),
                              np.random.uniform(0, 100, rows_per_store_month)).round(2)

        battery_mv = np.random.randint(3000, 4201, rows_per_store_month)

        for i in range(rows_per_store_month):
            if row_count_in_file >= max_rows_per_file:
                f.close()
                file_index += 1
                row_count_in_file = 0
                file_path = partition_dir / f"part-{file_index:04d}.csv"
                f = file_path.open("w", encoding="utf-8", newline="")
                writer = csv.writer(f)
                writer.writerow(sensors_schema.names)

            writer.writerow([
                ts_array[i],
                store_id,
                shelf_ids[i],
                Decimal(str(temperatures[i])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                Decimal(str(humidities[i])).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP),
                int(battery_mv[i])
            ])
            row_count_in_file += 1

        f.close()



def generate_sensors(out_dir: pathlib.Path, scale: float, seed: int, workers: int = 4):
    """Generate sensors data in parallel by store_id."""
    target_rows = int(8_000_000 * scale)  # mid-point between 5M and 10M
    num_stores = int(5000 * scale)
    months_span = 6
    today = datetime.today()
    month_list = [(today.replace(day=1) - timedelta(days=30 * i)).strftime("%Y-%m") for i in range(months_span)]
    rows_per_store_month = max(1, target_rows // (num_stores * months_span))
    max_rows_per_file = 50_000

    print(f"▶ Generating {target_rows} sensor readings for {num_stores} stores over {months_span} months using {workers} workers...")

    params_list = [
        (store_id, month_list, rows_per_store_month, max_rows_per_file, scale, seed, out_dir)
        for store_id in range(1, num_stores + 1)
    ]

    with ProcessPoolExecutor(max_workers=workers) as executor:
        results = list(executor.map(_generate_sensors_for_store, params_list))

    print(f"✅ Generated sensors for {len(results)} stores → {out_dir/'sensors'}")


def generate_exchange_rates(out_dir: pathlib.Path, scale: float, seed: int):
    random.seed(seed)

    # Target ~3 years daily
    days_span = int(365 * 3 * scale)
    end_date = date.today()
    start_date = end_date - timedelta(days=days_span - 1)

    currencies = ["USD", "EUR", "GBP", "NZD", "CNY"]

    # Base rates for each currency (relative to AUD)
    base_rates = {
        "USD": 0.65,
        "EUR": 0.60,
        "GBP": 0.52,
        "NZD": 1.08,
        "CNY": 4.70
    }

    # Create Excel workbook
    wb = Workbook()
    ws = wb.active
    ws.title = "exchange_rates"
    ws.append(exchange_rates_schema.names)  # Header from schema

    prev_rates = base_rates.copy()

    current_date = start_date
    while current_date <= end_date:
        # If weekend, reuse Friday's rates
        if current_date.weekday() >= 5:  # Saturday or Sunday
            day_rates = prev_rates
        else:
            # Generate new rates with small fluctuations
            day_rates = {}
            for cur in currencies:
                change_factor = random.uniform(-0.005, 0.005)  # ±0.5%
                new_rate = prev_rates[cur] * (1 + change_factor)
                day_rates[cur] = new_rate
            prev_rates = day_rates

        # Write rows for each currency
        for cur in currencies:
            rate_decimal = Decimal(day_rates[cur]).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
            ws.append([current_date.isoformat(), cur, rate_decimal])

        current_date += timedelta(days=1)

    # Save Excel file
    file_path = out_dir / "exchange_rates.xlsx"
    wb.save(file_path)

    print(f"✅ Generated {len(currencies) * days_span} exchange rates → {file_path}")



def _generate_shipments_chunk(params):
    chunk_index, rows_in_chunk, scale, seed, out_dir = params

    # Load orders_header in worker
    orders_path = out_dir / "orders_header"
    orders_df = pd.concat(pd.read_csv(f) for f in orders_path.rglob("*.csv"))
    orders_sample = orders_df[["order_id", "order_ts"]].to_numpy()

    safe_seed = (seed + chunk_index) % (2**32)
    random.seed(safe_seed)
    np.random.seed(safe_seed)

    carriers = ["DHL", "FedEx", "UPS", "Australia Post", "TNT"]

    shipment_ids = np.arange(
        chunk_index * rows_in_chunk + 1,
        chunk_index * rows_in_chunk + rows_in_chunk + 1,
        dtype=np.int64
    )

    chosen_idx = np.random.randint(0, orders_sample.shape[0], rows_in_chunk)
    order_ids = orders_sample[chosen_idx, 0]
    order_ts_list = [datetime.fromisoformat(ts) for ts in orders_sample[chosen_idx, 1]]

    # Inject ~1% invalid order_ids
    invalid_mask = np.random.rand(rows_in_chunk) < 0.01
    order_ids = np.where(
        invalid_mask,
        np.random.randint(order_ids.max() + 1, order_ids.max() + 1000, rows_in_chunk),
        order_ids
    )

    # Shipped_at: 1–5 days after order_ts
    shipped_at_list = [
        ts + timedelta(days=random.randint(1, 5), hours=random.randint(0, 23), minutes=random.randint(0, 59))
        for ts in order_ts_list
    ]

    # Delivered_at: 10% null, 5% late
    delivered_at_list = []
    for shipped_time in shipped_at_list:
        if random.random() < 0.10:
            delivered_at_list.append(None)
        else:
            if random.random() < 0.05:
                delivered_at_list.append(shipped_time + timedelta(days=random.randint(8, 20)))
            else:
                delivered_at_list.append(shipped_time + timedelta(days=random.randint(1, 7)))

    carrier_choices = np.random.choice(carriers, size=rows_in_chunk)
    ship_costs = [
        Decimal(random.uniform(5.00, 60.00)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        for _ in range(rows_in_chunk)
    ]

    table = pa.Table.from_arrays(
        [
            pa.array(shipment_ids, type=pa.int64()),
            pa.array(order_ids, type=pa.int64()),
            pa.array(carrier_choices, type=pa.string()),
            pa.array(shipped_at_list, type=pa.timestamp("us")),
            pa.array(delivered_at_list, type=pa.timestamp("us")),
            pa.array(ship_costs, type=pa.decimal128(12, 2)),
        ],
        schema=shipments_schema
    )

    ensure_dir(out_dir)
    file_path = out_dir / f"shipments_part-{chunk_index+1:04d}.parquet"
    pq.write_table(table, file_path)

    return f"✅ Shipments chunk {chunk_index+1} done"



def generate_shipments(out_dir: pathlib.Path, scale: float, seed: int, workers: int = 4):
    target_rows = int(1_000_000 * scale)
    rows_per_chunk = 100_000
    num_chunks = (target_rows + rows_per_chunk - 1) // rows_per_chunk

    # Load orders_header into memory once
    orders_path = out_dir / "orders_header"
    if not orders_path.exists():
        raise FileNotFoundError(f"orders_header not found at {orders_path}")

    print("▶ Loading orders_header for shipments linkage...")
    orders_df = pd.concat(
        pd.read_csv(f) for f in orders_path.rglob("*.csv")
    )
    orders_sample = orders_df[["order_id", "order_ts"]].values  # numpy array for speed

    params_list = [
        (chunk_index, rows_per_chunk if chunk_index < num_chunks - 1 else target_rows - rows_per_chunk * chunk_index,
        scale, seed, out_dir)
        for chunk_index in range(num_chunks)
    ]


    print(f"▶ Generating {target_rows} shipments in {num_chunks} chunks using {workers} workers...")
    with ProcessPoolExecutor(max_workers=workers) as executor:
        for idx, result in enumerate(executor.map(_generate_shipments_chunk, params_list), start=1):
            if idx % 5 == 0 or idx == num_chunks:
                print(f"Progress: {idx}/{num_chunks} chunks complete")

    print(f"✅ Generated {target_rows} shipments → {out_dir}")


# ---------------------------
# V1: Base Schema Generation
# ---------------------------
def generate_returns_v1(out_dir: pathlib.Path, scale: float, seed: int):
    random.seed(seed)
    np.random.seed(seed)
    fake = Faker("en_AU")

    target_rows = int(100_000 * scale)

    # --- Load orders_header and orders_lines ---
    orders_path = out_dir / "orders_header"
    if not orders_path.exists():
        raise FileNotFoundError(f"orders_header not found at {orders_path}")
    orders_df = pd.concat(pd.read_csv(f) for f in orders_path.rglob("*.csv"))

    lines_path = out_dir / "orders_lines"
    if not lines_path.exists():
        raise FileNotFoundError(f"orders_lines not found at {lines_path}")
    lines_df = pd.concat(pd.read_csv(f) for f in lines_path.rglob("*.csv"))

    # Merge to get valid order_id/product_id combos
    merged_df = pd.merge(lines_df, orders_df[["order_id", "order_ts"]], on="order_id", how="inner")
    merged_df["order_ts"] = pd.to_datetime(merged_df["order_ts"])

    # --- Sample returns ---
    sampled = merged_df.sample(n=target_rows, replace=True, random_state=seed)

    return_ids = np.arange(1, target_rows + 1, dtype=np.int64)
    order_ids = sampled["order_id"].values
    product_ids = sampled["product_id"].values
    return_ts = [
        order_date + timedelta(days=random.randint(1, 60))
        for order_date in sampled["order_ts"]
    ]
    qtys = np.random.randint(1, 5, size=target_rows)
    reasons = [random.choice(["Defective", "Wrong Item", "No Longer Needed", "Other"]) for _ in range(target_rows)]

    v1_df = pd.DataFrame({
        "return_id": return_ids,
        "order_id": order_ids,
        "product_id": product_ids,
        "return_ts": return_ts,
        "qty": qtys,
        "reason": reasons
    })

    returns_path = out_dir / "returns"
    write_deltalake(str(returns_path), v1_df, mode="overwrite")

    print(f"✅ Generated v1 returns ({len(v1_df)} rows) → {returns_path}")


# ---------------------------
# V2: Schema Evolution + UPSERT + DELETE
# ---------------------------
def generate_returns_v2(out_dir: pathlib.Path, scale: float, seed: int):
    random.seed(seed)
    np.random.seed(seed)

    returns_path = out_dir / "returns"
    if not returns_path.exists():
        raise FileNotFoundError(f"returns Delta table not found at {returns_path}")

    dt = DeltaTable(str(returns_path))
    v1_df = dt.to_pandas()

    target_rows = len(v1_df)
    order_ids = v1_df["order_id"].values
    product_ids = v1_df["product_id"].values
    return_ids = v1_df["return_id"].values

    # --- UPSERTS ---
    upsert_count = int(0.05 * target_rows)  # 5% updates
    insert_count = int(0.02 * target_rows)  # 2% inserts

    # Update some existing rows
    update_df = v1_df.sample(n=upsert_count, random_state=seed).copy()
    update_df["reason"] = "Updated Reason"
    update_df["return_reason_code"] = ["URC" + str(i) for i in range(upsert_count)]

    # Insert new rows
    new_ids = np.arange(target_rows + 1, target_rows + 1 + insert_count, dtype=np.int64)
    new_rows = pd.DataFrame({
        "return_id": new_ids,
        "order_id": np.random.choice(order_ids, size=insert_count),
        "product_id": np.random.choice(product_ids, size=insert_count),
        "return_ts": [datetime.now() - timedelta(days=random.randint(1, 30)) for _ in range(insert_count)],
        "qty": np.random.randint(1, 5, size=insert_count),
        "reason": ["Inserted Reason"] * insert_count,
        "return_reason_code": ["IRC" + str(i) for i in range(insert_count)]
    })

    # Combine updated and new rows
    v2_df = pd.concat([update_df, new_rows], ignore_index=True)

    # Write v2 with schema evolution
    write_deltalake(str(returns_path), v2_df, mode="append", schema_mode="merge")

    print(f"✅ Generated v2 returns (UPSERT + INSERT) → {returns_path}")

    # --- DELETE some rows ---
    delete_count = int(0.01 * target_rows)  # 1% deletes
    delete_ids = np.random.choice(return_ids, size=delete_count, replace=False)

    final_df = dt.to_pandas()
    final_df = final_df[~final_df["return_id"].isin(delete_ids)]
   

    # ✅ Reset index to avoid __index_level_0__ in schema
    final_df = final_df.reset_index(drop=True)

    write_deltalake(str(returns_path), final_df, mode="overwrite", schema_mode="merge")

    print(f"✅ Deleted {delete_count} rows from returns → {returns_path}")


# ---------- Dataset Registry ----------


DATASETS = {
    "customers": {"schema": customers_schema, "target_rows": 80000, "generator": generate_customers},
    "products": {"schema": products_schema, "target_rows": 25000, "generator": generate_products},
    "stores": {"schema": stores_schema, "target_rows": 5000, "generator": generate_stores},
    "suppliers": {"schema": suppliers_schema, "target_rows": 8000, "generator": generate_suppliers},
    "orders_header": {"schema": orders_header_schema, "target_rows": 1000000, "generator": generate_orders_header},
    "orders_lines": {"schema": orders_lines_schema,"target_rows": 4_000_000,"generator": generate_orders_lines},
    "events": {"schema": events_schema, "target_rows": 2_000_000, "generator": generate_events},
    "sensors": {"schema": sensors_schema, "target_rows": 8_000_000, "generator": generate_sensors},
    "exchange_rates": {"schema": exchange_rates_schema, "target_rows": 1100, "generator": generate_exchange_rates},
    "shipments": {"schema": shipments_schema, "target_rows": 1_000_000, "generator": generate_shipments},
    "returns_v1": {"schema": returns_day1_schema, "target_rows": 100_000, "generator": generate_returns_v1}, 
    #"returns_v2": {"schema": returns_day1_schema, "target_rows": 100_000, "generator": generate_returns_v2}

}

# Run the below only after generating returns_v1 for schema evolution demo
"""

DATASETS = {
    "returns_v2": {"schema": returns_day1_schema, "target_rows": 100_000, "generator": generate_returns_v2}
}
"""

def parse_args():
    parser = argparse.ArgumentParser(description="Generate synthetic retail datasets")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--out", type=str, default="data_raw", help="Output directory")
    parser.add_argument("--scale", type=float, default=0.01, help="Scale factor (1.0 = full dataset)")
    return parser.parse_args()


def main():
    args = parse_args()
    out_dir = pathlib.Path(args.out)
    ensure_dir(out_dir)

    for ds_name, ds_info in DATASETS.items():
        print(f"▶ Generating {ds_name} ...")

        if ds_name in ["orders_header","orders_lines", "events", "sensors", "exchange_rates", "shipments", "returns_v1", "returns_v2"]:  # partitioned datasets
            ds_info["generator"](out_dir, args.scale, args.seed)
        else:
            file_path = out_dir / f"{ds_name}.csv"
            ds_info["generator"](file_path, args.scale, args.seed)

    print(f"✅ All datasets generated in {out_dir}.")


if __name__ == "__main__":
    main()


