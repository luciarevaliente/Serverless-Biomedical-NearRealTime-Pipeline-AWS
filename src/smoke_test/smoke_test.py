"""
smoke_test.py — Verificació ràpida de la pipeline i les mètriques de latència
TFG: Arquitectura cloud distribuïda per al processament de dades biomèdiques
Lucía Revaliente Torres — UAB 2026

Execució:
    python smoke_test.py

Comprova:
    1. L'event s'envia correctament a Kinesis
    2. Lambda el processa i l'escriu a DynamoDB
    3. Les tres mètriques de latència tenen valors coherents:
         - kinesis_to_lambda_ms  > 0
         - processing_latency_ms > 0  (amb round, no int)
         - pipeline_latency_ms   > 0  i igual a la suma dels dos anteriors
"""

import boto3
import json
import time
import uuid
from decimal import Decimal

REGION         = 'eu-west-1'
STREAM_NAME    = 'tfg-biomedical-dev-stream'
DYNAMODB_TABLE = 'tfg-biomedical-dev-processed-data'
MAX_WAIT_S     = 60   # temps màxim esperant que Lambda processi l'event
POLL_INTERVAL  = 20    # cada quants segons consultem DynamoDB

kinesis  = boto3.client('kinesis',  region_name=REGION)
dynamodb = boto3.resource('dynamodb', region_name=REGION)
table    = dynamodb.Table(DYNAMODB_TABLE)

# ── 1. Enviar event ───────────────────────────────────────────────────────────
event_id = str(uuid.uuid4())
event = {
    "event_id":        event_id,
    "subject_id":      "S1",
    "device_id":       "EmpaticaE4",
    "sensor_type":     "EDA",
    "sampling_rate_hz": 4,
    "sensor_timestamp": time.time(),
    "value":           0.85,
    "unit":            "uS",
    "scenario":        "smoke",
    "schema_version":  "1.0",
    "run_id":          0,
}

response = kinesis.put_record(
    StreamName=STREAM_NAME,
    Data=json.dumps(event).encode('utf-8'),
    PartitionKey='S1#EDA'
)

print(f"\n── Smoke Test ───────────────────────────────────────────")
print(f"  event_id : {event_id}")
print(f"  ShardId  : {response['ShardId']}")
print(f"  Esperant que Lambda processi l'event (màx {MAX_WAIT_S}s)...")

# ── 2. Esperar i consultar DynamoDB ──────────────────────────────────────────
item = None
elapsed = 0
while elapsed < MAX_WAIT_S:
    time.sleep(POLL_INTERVAL)
    elapsed += POLL_INTERVAL
    try:
        result = table.get_item(Key={'event_id': event_id})
        if 'Item' in result:
            item = result['Item']
            break
    except Exception as e:
        print(f"  Error consultant DynamoDB: {e}")

# ── 3. Mostrar resultats ─────────────────────────────────────────────────────
if item is None:
    print(f"\n  ✗ FAIL — L'event no ha arribat a DynamoDB després de {MAX_WAIT_S}s")
    print(f"          Comprova que Lambda està activa i Kinesis trigger està enabled.")
else:
    def f(val):
        return float(val) if isinstance(val, Decimal) else val

    k2l  = f(item.get('kinesis_to_lambda_ms'))
    proc = f(item.get('processing_latency_ms'))
    pipe = f(item.get('pipeline_latency_ms'))

    print(f"\n  ✓ Event trobat a DynamoDB després de ~{elapsed}s")
    print(f"\n── Mètriques de latència ────────────────────────────────")
    print(f"  kinesis_to_lambda_ms  : {k2l} ms")
    print(f"  processing_latency_ms : {proc} ms")
    print(f"  pipeline_latency_ms   : {pipe} ms")
    print(f"────────────────────────────────────────────────────────")

    # Validacions
    errors = []
    if k2l is None:
        errors.append("kinesis_to_lambda_ms no existeix al record")
    elif k2l <= 0:
        errors.append(f"kinesis_to_lambda_ms={k2l} hauria de ser > 0")

    if proc is None:
        errors.append("processing_latency_ms no existeix al record")
    elif proc <= 0:
        errors.append(f"processing_latency_ms={proc} hauria de ser > 0 (comprova round vs int al handler)")

    if pipe is None:
        errors.append("pipeline_latency_ms no existeix al record")
    elif pipe <= 0:
        errors.append(f"pipeline_latency_ms={pipe} hauria de ser > 0")

    if k2l and proc and pipe:
        expected = round(k2l + proc, 3)
        diff = abs(pipe - expected)
        if diff > 1:
            errors.append(f"pipeline_latency_ms={pipe} no coincideix amb k2l+proc={expected} (diff={diff}ms)")

    if errors:
        print(f"\n  ✗ WARNINGS:")
        for e in errors:
            print(f"    - {e}")
    else:
        print(f"\n  ✓ Totes les mètriques són coherents")

print(f"────────────────────────────────────────────────────────\n")