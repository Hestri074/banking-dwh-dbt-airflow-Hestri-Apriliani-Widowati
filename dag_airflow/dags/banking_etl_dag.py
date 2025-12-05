from datetime import datetime, timedelta
import os
import csv

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator

import duckdb
import pandas as pd

# ============================================================================
# CONSTANTS AND PATHS (pakai struktur yang lama)
# ============================================================================

BASE_PATH = "/opt/airflow"

DATA_DIR = os.path.join(BASE_PATH, "data")
DATA_SOURCE_DIR = os.path.join(DATA_DIR, "source")     # 3 CSV soal ujian
RAW_DIR = os.path.join(DATA_DIR, "raw")                # output task save_raw_csv
PROCESSED_DIR = os.path.join(DATA_DIR, "processed")    # snapshot validasi

DBT_PROJECT_DIR = os.path.join(BASE_PATH, "dbt_projects", "banking_dw", "dev.duckdb")
DUCKDB_PATH = os.path.join(DATA_DIR, "banking.duckdb")

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)


# ============================================================================
# TASK 1: EXTRACT DATA (baca 3 CSV via DuckDB → join → push ke XCom)
# ============================================================================

def extract_data_func(**context):
    """
    Mengambil data dari 3 CSV (customers, accounts, transactions)
    menggunakan DuckDB, melakukan join, lalu push hasilnya ke XCom
    sebagai list of dict.
    """

    print("=" * 80)
    print("TASK 1: EXTRACT DATA")
    print("=" * 80)

    # Pastikan folder data ada
    os.makedirs(DATA_DIR, exist_ok=True)

    # Koneksi ke DuckDB (file di /opt/airflow/data/banking.duckdb)
    print(f"Connecting to DuckDB: {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH)

    # Path CSV
    customers_csv = os.path.join(DATA_SOURCE_DIR, "customers.csv")
    accounts_csv = os.path.join(DATA_SOURCE_DIR, "accounts.csv")
    transactions_csv = os.path.join(DATA_SOURCE_DIR, "transactions.csv")

    print("Registering CSV as tables...")
    con.execute(f"""
        CREATE OR REPLACE TABLE customers AS
        SELECT * FROM read_csv_auto('{customers_csv}', header=True);
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE accounts AS
        SELECT * FROM read_csv_auto('{accounts_csv}', header=True);
    """)
    con.execute(f"""
        CREATE OR REPLACE TABLE transactions AS
        SELECT * FROM read_csv_auto('{transactions_csv}', header=True);
    """)

    # Join transaksi + account + customer
    # catatan: di customers tidak ada customer_name, maka kita buat dari first_name + last_name
    query = """
        SELECT
            t.*,
            a.account_type,
            c.first_name || ' ' || c.last_name AS customer_name,
            c.risk_rating AS customer_segment
        FROM transactions t
        LEFT JOIN accounts a ON t.account_id = a.account_id
        LEFT JOIN customers c ON a.customer_id = c.customer_id
    """

    print("Executing query...")
    df = con.execute(query).df()
    con.close()

    print(f"✓ Successfully extracted {len(df)} records")
    print(f"✓ Columns: {list(df.columns)}")

    # Pastikan kolom tanggal/waktu string (aman untuk XCom)
    for col in ["transaction_date", "transaction_time"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Convert dataframe → list of dict
    records = df.to_dict(orient="records")

    # Push ke XCom
    context["ti"].xcom_push(
        key="raw_transactions",
        value=records,
    )

    print(f"✓ Data pushed to XCom with key 'raw_transactions'")
    print("=" * 80)


# ============================================================================
# TASK 2: SAVE RAW CSV (ambil dari XCom → simpan ke data/raw)
# ============================================================================

def save_raw_csv_func(**context):
    """
    Menyimpan data mentah yang diambil dari task extract_data
    ke folder data/raw dalam format CSV.

    Output:
        /opt/airflow/data/raw/transactions_raw.csv
    """
    print("=" * 80)
    print("TASK 2: SAVE RAW CSV")
    print("=" * 80)

    os.makedirs(RAW_DIR, exist_ok=True)

    print("Pulling data from XCom...")
    records = context["ti"].xcom_pull(
        key="raw_transactions",
        task_ids="extract_data",
    )

    if not records:
        raise ValueError("Tidak ada data yang diambil pada task extract_data")

    print(f"✓ Retrieved {len(records)} records from XCom")

    output_path = os.path.join(RAW_DIR, "transactions_raw.csv")
    fieldnames = list(records[0].keys())

    print(f"Writing to: {output_path}")
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    print(f"✓ Successfully saved {len(records)} records to {output_path}")
    print(f"✓ File size: {os.path.getsize(output_path)} bytes")
    print("=" * 80)


# ============================================================================
# TASK 3: RUN DBT TRANSFORM (pakai dbt di dbt_projects/banking_dw)
# ============================================================================

def run_dbt_command():
    """
    Dipanggil oleh BashOperator melalui bash_command.
    Di sini kita hanya dokumentasi; eksekusi nyata di bash_command.
    """
    pass


# ============================================================================
# TASK 4: VALIDATE OUTPUT (cek file raw, record count, null, dll)
# ============================================================================

def validate_output_func(**context):
    """
    Validasi hasil transformasi:
    - Cek file raw ada
    - Cek jumlah record > 0
    - Cek transaction_id tidak null/kosong
    - Cek kolom penting tersedia
    - Simpan snapshot hasil validasi ke data/processed
    """
    print("=" * 80)
    print("TASK 4: VALIDATE OUTPUT")
    print("=" * 80)

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    raw_path = os.path.join(RAW_DIR, "transactions_raw.csv")
    print(f"Checking file: {raw_path}")

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"File raw tidak ditemukan: {raw_path}")

    print("✓ File exists")

    # Baca dan validasi
    print("Reading and validating data...")
    rows = []
    with open(raw_path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)

    # Validasi 1: jumlah record
    if len(rows) == 0:
        raise ValueError("Validasi gagal: tidak ada record di transactions_raw.csv")

    print(f"✓ Total records: {len(rows)}")

    # Validasi 2: transaction_id tidak null/kosong
    null_count = 0
    for idx, row in enumerate(rows, start=1):
        tx_id = row.get("transaction_id")
        if tx_id is None or str(tx_id).strip() == "":
            null_count += 1
            print(f"Peringatan: transaction_id kosong pada baris ke-{idx}")

    if null_count > 0:
        raise ValueError(
            f"Validasi gagal: ditemukan {null_count} transaction_id yang kosong"
        )

    print("✓ All transaction_id values are valid (not null/empty)")

    # Validasi 3: cek kolom penting
    required_columns = [
        "transaction_id", "account_id", "transaction_date",
        "amount", "transaction_type"
    ]

    if rows:
        available_columns = set(rows[0].keys())
        missing_columns = set(required_columns) - available_columns

        if missing_columns:
            raise ValueError(
                f"Validasi gagal: kolom yang hilang: {missing_columns}"
            )

        print(f"✓ All required columns present: {required_columns}")

    # Simpan snapshot yang tervalidasi
    validated_path = os.path.join(
        PROCESSED_DIR, "transactions_validated_snapshot.csv"
    )

    print(f"Saving validated snapshot to: {validated_path}")
    fieldnames = list(rows[0].keys())
    with open(validated_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"✓ Validated snapshot saved: {validated_path}")
    print(f"✓ File size: {os.path.getsize(validated_path)} bytes")
    print("=" * 80)
    print("ALL VALIDATIONS PASSED")
    print("=" * 80)


# ============================================================================
# DAG CONFIGURATION
# ============================================================================

default_args = {
    "owner": "hestri",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="banking_etl_dag",
    default_args=default_args,
    description="DAG ETL sederhana untuk data perbankan (customers, accounts, transactions) + dbt",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # manual trigger (sesuai ujian, bisa kamu ganti @daily kalau mau)
    catchup=False,
    tags=["banking", "etl", "dbt", "exam"],
    max_active_runs=1,
) as dag:

    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_func,
    )

    save_raw_csv = PythonOperator(
        task_id="save_raw_csv",
        python_callable=save_raw_csv_func,
    )

    run_dbt_transform = BashOperator(
        task_id="run_dbt_transform",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            "dbt run --profiles-dir ."
        ),
    )

    validate_output = PythonOperator(
        task_id="validate_output",
        python_callable=validate_output_func,
    )

    # Urutan ETL sesuai soal:
    extract_data >> save_raw_csv >> run_dbt_transform >> validate_output
