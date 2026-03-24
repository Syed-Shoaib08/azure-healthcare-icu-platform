import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient
import io

# ── CONNECTION ──────────────────────────────────────────────
STORAGE_CONNECTION_STR = "Cannot share the connection string here for security reasons. Please replace with your own connection string."
SILVER_CONTAINER = "silver-layer"
GOLD_CONTAINER   = "gold-layer"

blob_service   = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STR)
silver_client  = blob_service.get_container_client(SILVER_CONTAINER)
gold_client    = blob_service.get_container_client(GOLD_CONTAINER)

# ── CLINICAL RISK SCORING ────────────────────────────────────
def calculate_risk_score(row):
    score = 0
    hr = row['heart_rate']
    if hr < 40 or hr > 150:         score += 3
    elif hr < 60 or hr > 120:       score += 2
    elif hr < 50 or hr > 100:       score += 1

    spo2 = row['oxygen_saturation']
    if spo2 < 85:                   score += 3
    elif spo2 < 90:                 score += 2
    elif spo2 < 95:                 score += 1

    temp = row['temperature']
    if temp > 104 or temp < 95:     score += 3
    elif temp > 102 or temp < 96.8: score += 2
    elif temp > 100.4 or temp < 97: score += 1

    sbp = row['systolic_bp']
    if sbp > 180 or sbp < 80:      score += 3
    elif sbp > 160 or sbp < 90:    score += 2
    elif sbp > 140 or sbp < 100:   score += 1

    dbp = row['diastolic_bp']
    if dbp > 120 or dbp < 50:      score += 2
    elif dbp > 90 or dbp < 60:     score += 1

    rr = row['respiratory_rate']
    if rr > 30 or rr < 8:          score += 3
    elif rr > 25 or rr < 12:       score += 2
    elif rr > 20:                   score += 1

    return score

def assign_risk_level(score):
    if score >= 8:   return 'Critical'
    elif score >= 5: return 'High'
    elif score >= 2: return 'Medium'
    else:            return 'Low'

def assign_alert_flag(row):
    return (
        (row['heart_rate'] > 100)       |
        (row['heart_rate'] < 60)        |
        (row['oxygen_saturation'] < 95) |
        (row['temperature'] > 100.4)    |
        (row['temperature'] < 96.8)     |
        (row['systolic_bp'] > 140)      |
        (row['systolic_bp'] < 90)       |
        (row['diastolic_bp'] > 90)      |
        (row['diastolic_bp'] < 60)      |
        (row['respiratory_rate'] > 20)  |
        (row['respiratory_rate'] < 12)
    )

# ── STEP 1: READ LATEST PARTITION ONLY FROM SILVER ──────────
print("Reading silver layer (latest partition only)...")
all_blobs = list(silver_client.list_blobs())

# Extract unique partitions and find the latest one
partitions = set()
for blob in all_blobs:
    parts = blob.name.split('/')
    if len(parts) >= 4:
        partition = '/'.join(parts[:4])
        partitions.add(partition)

latest_partition = sorted(partitions)[-1] if partitions else None
blob_list = [b for b in all_blobs if latest_partition and b.name.startswith(latest_partition)]

print(f"   Total files in silver  : {len(all_blobs)}")
print(f"   Latest partition       : {latest_partition}")
print(f"   Files in latest batch  : {len(blob_list)}\n")

frames = []
skipped = 0
for blob in blob_list:
    blob_data = silver_client.get_blob_client(blob.name).download_blob().readall()
    if not blob_data or len(blob_data) < 100:
        skipped += 1
        continue
    try:
        buffer = io.BytesIO(blob_data)
        df = pd.read_parquet(buffer, engine='pyarrow')
        frames.append(df)
    except Exception as e:
        skipped += 1
        continue

print(f"   Skipped empty/corrupt files: {skipped}")
print(f"   Valid parquet files loaded : {len(frames)}\n")

df = pd.concat(frames, ignore_index=True)
print(f"Loaded {len(df)} clean records from silver\n")

# ── STEP 2: RECALCULATE RISK LEVEL ──────────────────────────
print("Recalculating clinical risk levels...")

df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True)

# Latest reading per patient
latest = df.sort_values('timestamp').groupby('patient_id').last().reset_index()

latest['risk_score']          = latest.apply(calculate_risk_score, axis=1)
latest['risk_level']          = latest['risk_score'].apply(assign_risk_level)
latest['alert_flag']          = latest.apply(assign_alert_flag, axis=1)
latest['processed_timestamp'] = datetime.utcnow().isoformat()

print(f"   Patients processed : {len(latest)}")
print(f"   Critical patients  : {len(latest[latest['risk_level'] == 'Critical'])}")
print(f"   High risk patients : {len(latest[latest['risk_level'] == 'High'])}")
print(f"   Medium patients    : {len(latest[latest['risk_level'] == 'Medium'])}")
print(f"   Low risk patients  : {len(latest[latest['risk_level'] == 'Low'])}")
print(f"   Patients on alert  : {len(latest[latest['alert_flag'] == True])}\n")

# ── STEP 3: GOLD TABLE 2 — HOURLY VITALS AGGREGATION ────────
print("Building Gold Table 2: Hourly Aggregations...")

# Join risk_level from latest back to df so hourly groupby can use it
risk_map = latest[['patient_id', 'risk_level']].set_index('patient_id')['risk_level']
df['risk_level'] = df['patient_id'].map(risk_map)
df = df.dropna(subset=['risk_level'])  # drop any patients not in latest

df['hour'] = df['timestamp'].dt.floor('h')

hourly = df.groupby(['hour', 'risk_level']).agg(
    patient_count        = ('patient_id', 'nunique'),
    avg_heart_rate       = ('heart_rate', 'mean'),
    avg_oxygen_sat       = ('oxygen_saturation', 'mean'),
    avg_systolic_bp      = ('systolic_bp', 'mean'),
    avg_diastolic_bp     = ('diastolic_bp', 'mean'),
    avg_temperature      = ('temperature', 'mean'),
    avg_respiratory_rate = ('respiratory_rate', 'mean'),
    max_heart_rate       = ('heart_rate', 'max'),
    min_oxygen_sat       = ('oxygen_saturation', 'min')
).reset_index()

hourly = hourly.round(2)
print(f"   Hourly aggregation rows: {len(hourly)}\n")

# ── STEP 4: GOLD TABLE 3 — ICU SUMMARY METRICS ──────────────
print("Building Gold Table 3: ICU Summary Metrics...")

summary = pd.DataFrame([{
    'report_timestamp'     : datetime.utcnow().isoformat(),
    'total_patients'       : len(latest),
    'critical_count'       : len(latest[latest['risk_level'] == 'Critical']),
    'high_count'           : len(latest[latest['risk_level'] == 'High']),
    'medium_count'         : len(latest[latest['risk_level'] == 'Medium']),
    'low_count'            : len(latest[latest['risk_level'] == 'Low']),
    'alert_count'          : int(latest['alert_flag'].sum()),
    'avg_heart_rate'       : round(df['heart_rate'].mean(), 2),
    'avg_oxygen_sat'       : round(df['oxygen_saturation'].mean(), 2),
    'avg_systolic_bp'      : round(df['systolic_bp'].mean(), 2),
    'avg_temperature'      : round(df['temperature'].mean(), 2),
    'avg_respiratory_rate' : round(df['respiratory_rate'].mean(), 2),
}])

print(f"   ICU Summary built \n")

# ── STEP 5: WRITE ALL 3 GOLD TABLES (OVERWRITE) ─────────────
print(" Writing to gold layer (overwrite mode)...")

def write_parquet(df, path):
    for col in df.columns:
        if 'timestamp' in col.lower() and df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], utc=True)
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize('UTC') if df[col].dt.tz is None else df[col]
                df[col] = df[col].astype('datetime64[us, UTC]')
            except Exception:
                df[col] = pd.to_datetime(df[col], utc=True).astype('datetime64[us, UTC]')

    table = pa.Table.from_pandas(df, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, coerce_timestamps='us', allow_truncated_timestamps=True)
    buffer.seek(0)
    gold_client.get_blob_client(path).upload_blob(buffer.read(), overwrite=True)
    print(f" Written: {path}  ({len(df)} rows)")

write_parquet(latest,  "patient_risk_summary/patient_risk_summary.parquet")
write_parquet(hourly,  "hourly_aggregations/hourly_aggregations.parquet")
write_parquet(summary, "icu_summary/icu_summary.parquet")

# ── SUMMARY ─────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  SILVER → GOLD COMPLETE")
print(f"{'='*55}")
print(f"  Total patients      : {summary['total_patients'].iloc[0]}")
print(f"  Critical patients   : {summary['critical_count'].iloc[0]}")
print(f"  High risk patients  : {summary['high_count'].iloc[0]}")
print(f"  Patients on alert   : {summary['alert_count'].iloc[0]}")
print(f"  Avg heart rate      : {summary['avg_heart_rate'].iloc[0]} bpm")
print(f"  Avg oxygen sat      : {summary['avg_oxygen_sat'].iloc[0]} %")
print(f"  Gold tables written : 3")
print(f"  Gold layer          : Ready for Power BI")
print(f"{'='*55}")