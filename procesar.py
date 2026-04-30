import boto3
import datetime
import os

REGION = "us-east-1"
TABLA = "RegistroEventos"
LOG_FILE = "backup.log"

def timestamp():
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

client_events = boto3.client("events", region_name=REGION)
client_dynamo = boto3.resource("dynamodb", region_name=REGION)
tabla = client_dynamo.Table(TABLA)

criticos = []

with open("datos.txt", "r") as f:
    next(f)
    for line in f:
        parts = line.strip().split(",")
        if len(parts) < 2:
            continue
        estado, temp = parts[0], int(parts[1])

        if temp > 40:
            criticos.append((estado, temp))

            client_events.put_events(
                Entries=[{
                    "Source": "custom.pipeline",
                    "DetailType": "temperatura_alta",
                    "Detail": f'{{"estado": "{estado}", "temp": {temp}}}'
                }]
            )
            print(f"[EVENTO] Enviado para {estado} — {temp}C")

            tabla.put_item(Item={
                "id": estado,
                "temperatura": str(temp),
                "status": "CRITICO",
                "timestamp": timestamp(),
                "procesado_por": os.environ.get("CODEBUILD_BUILD_ID", "local")
            })
            print(f"[DYNAMO] Guardado {estado} en RegistroEventos")

log_entry = f"""
========================================
BACKUP LOG — {timestamp()}
========================================
Archivo procesado : datos.txt
Estados revisados : 10
Estados criticos  : {len(criticos)} (temperatura > 40C)
Detalle:
"""
for e, t in criticos:
    log_entry += f"  - {e}: {t}C\n"
log_entry += f"Build ID: {os.environ.get('CODEBUILD_BUILD_ID', 'local')}\n"
log_entry += "=" * 40 + "\n"

with open(LOG_FILE, "a") as f:
    f.write(log_entry)

print(f"[LOG] backup.log actualizado")
print(f"[OK] {len(criticos)} estados criticos procesados")
