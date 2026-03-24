import json
import random
import uuid
from datetime import datetime, timezone
from azure.storage.blob import BlobServiceClient

# ── CONNECTION ──────────────────────────────────────────────
STORAGE_CONNECTION_STR = "Cannot share the connection string here for security reasons. Please replace with your own connection string."
BRONZE_CONTAINER = "bronze-layer"

blob_service   = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STR)
bronze_client  = blob_service.get_container_client(BRONZE_CONTAINER)

# ── CONFIGURATION ───────────────────────────────────────────
NUM_PATIENTS   = 50   # Fresh patients per run
PATIENT_PREFIX = "PT"

# ── CLINICAL VITAL GENERATORS ───────────────────────────────
def generate_patient_vitals(patient_id):
    """
    Generates realistic ICU patient vitals with weighted distribution:
    - 60% Low risk (normal ranges)
    - 25% Medium risk (borderline values)
    - 10% High risk (abnormal values)
    -  5% Critical risk (life-threatening values)
    """
    risk_roll = random.random()

    if risk_roll < 0.60:       # Low risk — normal vitals
        heart_rate         = random.randint(62, 98)
        oxygen_saturation  = round(random.uniform(96.0, 99.9), 1)
        temperature        = round(random.uniform(97.5, 99.9), 1)
        systolic_bp        = random.randint(105, 135)
        diastolic_bp       = random.randint(65, 85)
        respiratory_rate   = random.randint(13, 19)

    elif risk_roll < 0.85:     # Medium risk — borderline vitals
        heart_rate         = random.randint(55, 115)
        oxygen_saturation  = round(random.uniform(93.0, 96.5), 1)
        temperature        = round(random.uniform(99.5, 101.5), 1)
        systolic_bp        = random.randint(90, 155)
        diastolic_bp       = random.randint(55, 95)
        respiratory_rate   = random.randint(10, 23)

    elif risk_roll < 0.95:     # High risk — abnormal vitals
        heart_rate         = random.randint(40, 140)
        oxygen_saturation  = round(random.uniform(88.0, 93.5), 1)
        temperature        = round(random.uniform(101.5, 104.0), 1)
        systolic_bp        = random.randint(75, 175)
        diastolic_bp       = random.randint(45, 110)
        respiratory_rate   = random.randint(8, 28)

    else:                      # Critical risk — life-threatening vitals
        heart_rate         = random.choice([
                                random.randint(25, 45),
                                random.randint(145, 180)
                             ])
        oxygen_saturation  = round(random.uniform(78.0, 88.5), 1)
        temperature        = round(random.uniform(104.5, 108.0), 1)
        systolic_bp        = random.choice([
                                random.randint(55, 80),
                                random.randint(180, 210)
                             ])
        diastolic_bp       = random.choice([
                                random.randint(30, 55),
                                random.randint(115, 135)
                             ])
        respiratory_rate   = random.choice([
                                random.randint(4, 8),
                                random.randint(30, 40)
                             ])

    return {
        "event_id"          : str(uuid.uuid4()),
        "patient_id"        : patient_id,
        "timestamp"         : datetime.now(timezone.utc).isoformat(),
        "heart_rate"        : heart_rate,
        "oxygen_saturation" : oxygen_saturation,
        "temperature"       : temperature,
        "systolic_bp"       : systolic_bp,
        "diastolic_bp"      : diastolic_bp,
        "respiratory_rate"  : respiratory_rate,
        "ward"              : random.choice(["ICU-A", "ICU-B", "ICU-C"]),
        "device_id"         : f"MONITOR-{random.randint(100, 999)}",
        "source"            : "simulated_daily_run"
    }

# ── GENERATE AND WRITE TO BRONZE ────────────────────────────
print("🏥 Starting daily ICU data simulation...")
print(f"   Generating {NUM_PATIENTS} patient vitals...\n")

now        = datetime.now(timezone.utc)
date_path  = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
written    = 0
errors     = 0

for i in range(1, NUM_PATIENTS + 1):
    patient_id = f"{PATIENT_PREFIX}{str(random.randint(1000, 9999)).zfill(6)}"
    vitals     = generate_patient_vitals(patient_id)
    blob_name  = f"{date_path}/vitals_{vitals['event_id']}.json"

    try:
        bronze_client.get_blob_client(blob_name).upload_blob(
            json.dumps(vitals, indent=2),
            overwrite=True
        )
        written += 1
    except Exception as e:
        print(f"   ❌ Error writing patient {patient_id}: {e}")
        errors += 1

# ── SUMMARY ─────────────────────────────────────────────────
print(f"{'='*55}")
print(f"  DAILY SIMULATION COMPLETE")
print(f"{'='*55}")
print(f"  Patients generated  : {NUM_PATIENTS}")
print(f"  Files written       : {written}")
print(f"  Errors              : {errors}")
print(f"  Bronze path         : {date_path}/")
print(f"  Timestamp           : {now.isoformat()}")
print(f"  Status              : {'✅ Ready for pipeline' if errors == 0 else '⚠️ Check errors above'}")
print(f"{'='*55}")