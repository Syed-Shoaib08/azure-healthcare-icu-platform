import json
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import io

# ── CONNECTION ──────────────────────────────────────────────
STORAGE_CONNECTION_STR = "Cannot share the connection string here for security reasons. Please replace with your own connection string."
BRONZE_CONTAINER = "bronze-layer"
SILVER_CONTAINER = "silver-layer"

blob_service   = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STR)
bronze_client  = blob_service.get_container_client(BRONZE_CONTAINER)
silver_client  = blob_service.get_container_client(SILVER_CONTAINER)

# ── STEP 1: READ LATEST PARTITION ONLY FROM BRONZE ──────────
print("Reading bronze layer (latest partition only)...")
all_blobs = list(bronze_client.list_blobs())

# Extract unique partitions and find the latest one
partitions = set()
for blob in all_blobs:
    parts = blob.name.split('/')
    if len(parts) >= 4:
        partition = '/'.join(parts[:4])  # year=.../month=.../day=.../hour=...
        partitions.add(partition)

latest_partition = sorted(partitions)[-1] if partitions else None
blob_list = [b for b in all_blobs if latest_partition and b.name.startswith(latest_partition)]

print(f"   Total files in bronze  : {len(all_blobs)}")
print(f"   Latest partition       : {latest_partition}")
print(f"   Files in latest batch  : {len(blob_list)}\n")

records = []
skipped = 0
for blob in blob_list:
    blob_data = bronze_client.get_blob_client(blob.name).download_blob().readall()
    if not blob_data or blob_data.strip() == b'':
        skipped += 1
        continue
    try:
        record = json.loads(blob_data)
        records.append(record)
    except json.JSONDecodeError:
        skipped += 1
        continue

print(f"   Skipped empty/corrupt files: {skipped}")
print(f"   Valid records loaded       : {len(records)}\n")

df_raw = pd.DataFrame(records)
print(f"Loaded {len(df_raw)} records into DataFrame")
print(f"   Columns: {list(df_raw.columns)}\n")

# ── STEP 2: DATA QUALITY CHECKS ─────────────────────────────
print(" Running data quality checks...")

total_before = len(df_raw)

# 2a. Remove exact duplicates
df = df_raw.drop_duplicates()
dup_removed = total_before - len(df)
print(f"   Duplicates removed          : {dup_removed}")

# 2b. Parse timestamp
df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, format='ISO8601')

# 2c. Handle blood_pressure — support both formats
if 'blood_pressure' in df.columns:
    mask = df['blood_pressure'].notna() & df['blood_pressure'].str.contains('/', na=False)
    df.loc[mask, 'systolic_bp']  = df.loc[mask, 'blood_pressure'].str.split('/').str[0].astype(int)
    df.loc[mask, 'diastolic_bp'] = df.loc[mask, 'blood_pressure'].str.split('/').str[1].astype(int)
    df = df.drop(columns=['blood_pressure'])

df['systolic_bp']  = pd.to_numeric(df['systolic_bp'],  errors='coerce').fillna(0).astype(int)
df['diastolic_bp'] = pd.to_numeric(df['diastolic_bp'], errors='coerce').fillna(0).astype(int)
df = df[(df['systolic_bp'] > 0) & (df['diastolic_bp'] > 0)]

# 2d. Validate vital sign ranges (clinical thresholds)
invalid_hr   = df[(df['heart_rate'] < 20)        | (df['heart_rate'] > 250)].index
invalid_o2   = df[(df['oxygen_saturation'] < 50)  | (df['oxygen_saturation'] > 100)].index
invalid_temp = df[(df['temperature'] < 90)         | (df['temperature'] > 110)].index
invalid_rr   = df[(df['respiratory_rate'] < 5)    | (df['respiratory_rate'] > 60)].index
invalid_sbp  = df[(df['systolic_bp'] < 50)         | (df['systolic_bp'] > 250)].index
invalid_dbp  = df[(df['diastolic_bp'] < 30)        | (df['diastolic_bp'] > 150)].index

all_invalid = (invalid_hr
               .union(invalid_o2)
               .union(invalid_temp)
               .union(invalid_rr)
               .union(invalid_sbp)
               .union(invalid_dbp))

df = df.drop(index=all_invalid)

print(f"   Out-of-range vitals removed : {len(all_invalid)}")
print(f"   Records after cleaning      : {len(df)}\n")

# ── STEP 3: ENRICH / TRANSFORM ───────────────────────────────
print("  Enriching data...")

df['ingestion_timestamp'] = datetime.utcnow().isoformat()
df['data_quality_flag']   = 'PASSED'

df['year']  = df['timestamp'].dt.year
df['month'] = df['timestamp'].dt.month
df['day']   = df['timestamp'].dt.day
df['hour']  = df['timestamp'].dt.hour

print(f"   Added: ingestion_timestamp, data_quality_flag\n")

# ── STEP 4: OVERWRITE SILVER LAYER (DELETE OLD, WRITE FRESH) ─
print(" Writing to silver layer (overwrite mode)...")

# Delete only actual parquet files (skip directory entries)
existing_silver = list(silver_client.list_blobs())
deleted = 0
for blob in existing_silver:
    if blob.name.endswith('.parquet'):
        try:
            silver_client.get_blob_client(blob.name).delete_blob()
            deleted += 1
        except Exception as e:
            print(f"   ⚠️ Could not delete {blob.name}: {e}")
print(f"    Deleted {deleted} old silver parquet file(s)")

# Write fresh partitions — overwrite=True handles any remaining conflicts
part_groups = df.groupby(['year', 'month', 'day', 'hour'])

files_written = 0
for (year, month, day, hour), partition_df in part_groups:
    clean_df = partition_df.drop(columns=['year', 'month', 'day', 'hour'])

    buffer = io.BytesIO()
    clean_df.to_parquet(buffer, index=False, engine='pyarrow')
    buffer.seek(0)

    blob_path = f"year={year}/month={month:02d}/day={day:02d}/hour={hour:02d}/vitals_cleaned.parquet"
    silver_client.get_blob_client(blob_path).upload_blob(buffer.read(), overwrite=True)
    print(f"    Written: {blob_path}  ({len(clean_df)} records)")
    files_written += 1

# ── SUMMARY ──────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  BRONZE → SILVER COMPLETE")
print(f"{'='*55}")
print(f"  Input records  : {total_before}")
print(f"  Clean records  : {len(df)}")
print(f"  Dropped records: {total_before - len(df)}")
print(f"  Parquet files  : {files_written}")
print(f"  Silver layer   :  Ready")
print(f"{'='*55}")