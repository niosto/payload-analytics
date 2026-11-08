"""Modelo de costo mensual (USD) de la plataforma Payload Analytics en AWS us-east-2.

Calcula el costo por componente para una malla de escenarios de ingesta y de consumo,
genera tablas en Markdown (tablas.md, resultados.json) y graficos PNG (charts/).

Uso:  python cost_model.py          (lee prices.json; no llama a AWS)

Todos los supuestos son constantes en MAYUSCULAS al inicio del archivo. Unidades:
USD por mes de 30 dias (720 h), GB = 10^9 bytes de forma aproximada (1 GB ~ 1024 MB).
"""
import json
import math
from pathlib import Path

HERE = Path(__file__).parent
PRICES = json.loads((HERE / "prices.json").read_text(encoding="utf-8"))

# =========================================================================================
# SUPUESTOS (todos modificables). Fuente: [R] README/IaC del repositorio, [M] medicion del
# equipo, [A] supuesto del analisis (justificado en README.md), [P] prices.json.
# =========================================================================================
DAYS = 30                       # dias por mes de calculo (coincide con la ventana de Cost Explorer)
HOURS = 24 * DAYS               # 720 h

# --- Ingesta batch -------------------------------------------------------------------------
BATCH_GB_DAY_TODAY = 0.05       # [M] un JSON de ~51 MB por corrida, GB/dia
RUNS_PER_DAY = 1.0              # [A] corridas por dia (un archivo por dia en regimen)
SFN_TRANSITIONS_PER_RUN = 6     # [R] transiciones de estado Step Functions por corrida
# Glue: duracion = fijo + trabajo variable / workers. El trabajo variable es lineal en GB.
GLUE_FIXED_MIN = 3.5            # [A] arranque de Spark + lectura de secreto + conexion JDBC, min por corrida
GLUE_VAR_DPU_MIN_PER_GB = 20.0  # [A] DPU-minutos de trabajo por GB (hoy: 0.05 GB -> 1 DPU-min; con 2 workers 3.5+0.5 = 4 min [M])
GLUE_TARGET_VAR_MIN = 15.0      # [A] se agregan workers para que la parte variable dure <= 15 min
GLUE_MAX_WORKERS = 100          # [A] tope de workers (limite practico de la cuenta)
GLUE_MIN_WORKERS = 2            # [R] G.1X x 2 workers, 1 DPU cada uno
GLUE_MIN_BILLED_MIN = 1.0       # [P] facturacion minima de 1 minuto
FILE_SPLIT_LIMIT_GB = 1.0       # [A] un solo JSON multilinea no es divisible: sobre 1 GB por archivo hay que partirlo
# dbt en Fargate: el computo real ocurre en RDS; el contenedor solo espera las sentencias SQL.
DBT_FIXED_MIN = 2.0             # [A] descarga de imagen + dbt seed/parse/test, min por corrida
DBT_MIN_PER_GB = 10.0           # [A] min por GB de entrada (hoy: 2.0 + 0.5 = 2.5 min [M]); lineal en el volumen
FARGATE_VCPU = 0.5              # [R] vCPU de la tarea dbt
FARGATE_GB = 1.0                # [R] GB de memoria de la tarea dbt
DBT_RDS_VCPU = 1.0              # [A] vCPU promedio que consume dbt en RDS mientras corre
DBT_SLA_MIN = 60.0              # [A] una corrida batch (Glue + dbt) debe terminar en <= 60 min
# S3
S3_RETENTION_DAYS = 90          # [A] dias que se conserva el JSON en processed/ (hoy no hay ciclo de vida)
S3_REQUESTS_PER_RUN = 20        # [A] PUT/COPY/LIST por corrida
S3_GETS_PER_RUN = 10            # [A] GET por corrida
# Logs
LOG_FIXED_MB_PER_RUN = 25.0     # [A] MB de logs (Glue + ECS) por corrida, independientes del volumen
LOG_MB_PER_GB = 5.0             # [A] MB de logs adicionales por GB procesado
LOG_RETENTION_DAYS = 14         # [R] retencion de CloudWatch Logs (ecs.yml)
# Monitoreo, secretos, registro
CW_ALARMS = 5                   # [R] "unas pocas" alarmas
CW_DASHBOARDS = 1               # [R]
CW_CUSTOM_METRICS = 4           # [R]
SECRETS = 2                     # [R]
SECRET_API_CALLS_PER_RUN = 4    # [A]
ECR_GB = 0.3                    # [R] ~6 imagenes, ~300 MB
SNS_EMAILS_PER_MONTH = 5        # [A] alertas de falla
EVENTS_PER_RUN = 1              # [R] un evento EventBridge por archivo
# Capa gratuita permanente (no vence a los 12 meses). Poner False para ver el costo "sin gratuidad".
APPLY_FREE_TIER = True
FREE_DTO_GB = 100               # [P] 100 GB/mes de transferencia saliente gratis
FREE_SFN_TRANSITIONS = 4000     # [P] transiciones gratis por mes
FREE_CW_ALARMS = 10
FREE_CW_DASHBOARDS = 3
FREE_CW_METRICS = 10
FREE_CW_LOGS_GB = 5
FREE_SNS_EMAILS = 1000

# --- Ingesta streaming ---------------------------------------------------------------------
STREAM_EPS_TODAY = 2.0          # [M] transacciones/s hoy
FX_ROWS_PER_S = 1.5             # [M] filas de tipo de cambio/s (fijo: depende de las monedas, no del volumen de transacciones)
TX_ROW_BYTES = 369              # [M] bytes por fila de transaccion en RDS (incluye indices)
FX_ROW_BYTES = 288              # [M] bytes por fila FX en RDS (incluye indices)
STREAM_HOT_DAYS = 7             # [A] dias de streaming que se conservan en RDS (hoy no hay purga)
STREAM_ARCHIVE = True           # [A] lo anterior se archiva a S3 en Parquet
STREAM_ARCHIVE_RATIO = 0.2      # [A] tamano en Parquet / tamano en RDS
STREAM_ARCHIVE_DAYS = 365       # [A] retencion del archivo en S3
KAFKA_MSG_BYTES = 600           # [A] bytes por evento JSON en el broker
KAFKA_RETENTION_DAYS = 3        # [A] retencion en MSK/Kinesis extendido
KAFKA_REPLICATION = 3           # [A] factor de replicacion de MSK (3 brokers/AZ)
MSK_BROKERS = 3                 # [A] brokers (uno por AZ)
MSK_SMALL_MAX_MBPS = 0.1        # [A] sobre 0.1 MB/s se pasa de kafka.t3.small a kafka.m7g.large
MSK_PARTITIONS = 4              # [A] particiones en MSK Serverless (2 topics x 2)
KINESIS_SHARD_MBPS = 1.0        # [P] limite de escritura por shard: 1 MB/s
KINESIS_SHARD_RECS = 1000       # [P] limite de escritura por shard: 1000 registros/s
GLUE_STREAM_ROWS_PER_DPU = 500  # [A] filas/s que sostiene 1 DPU de Glue Streaming (join + escritura JDBC)
GLUE_STREAM_MIN_DPU = 2         # [P] minimo de Glue Streaming: 2 DPU, 24x7
STREAM_LOG_GB_MONTH = 3.0       # [A] logs de Glue Streaming
STREAM_UPTIME = 1.0             # [A] fraccion del mes que el streaming corre en AWS (1.0 = 24x7)
GLUE_PRICE_MULT = 1.0           # multiplicador de sensibilidad del precio del DPU (1.0 = precio de prices.json)

# --- RDS: regla de dimensionamiento ----------------------------------------------------------
BASE_DB_GB = 0.5                # [A] catalogo, semillas, dbt, estadisticas
BATCH_DB_FACTOR = 3.0           # [A] staging + Silver + Gold ~ 3x el archivo (solo el ultimo se conserva: se reemplaza)
STORAGE_HEADROOM = 1.25         # [A] almacenamiento aprovisionado >= 1.25 x tamano (vacuum, orden, WAL)
MIN_STORAGE_GB = 20             # [R] gp2 de 20 GB
MEM_DB_RATIO = 10.0             # [A] tamano de la base <= 10 x RAM de la instancia (el conjunto caliente cabe en cache)
CPU_UTIL_TARGET = 0.7           # [A] carga promedio <= 70 % de la CPU sostenida (linea base)
STREAM_CPU_S_PER_ROW = 0.001    # [A] vCPU-s por fila escrita (upsert con 3 indices ~ 1 ms)
ANALYST_CPU = 0.02              # [A] vCPU promedio por analista concurrente (2 % de ciclo de trabajo)
DASH_QUERIES = 10               # [A] consultas por actualizacion del tablero
DASH_QUERY_CPU_S = 3.0          # [A] vCPU-s por consulta
DASH_PAYLOAD_GB = 0.05          # [A] GB transferidos por actualizacion completa (Gold agregado, ~ un archivo de entrada)
ADHOC_GB_PER_ANALYST = 0.5      # [A] GB/mes que descarga cada analista con SQL ad hoc
REFRESHES_PER_DAY = 8           # [A] actualizaciones del tablero por dia (base)
ANALYSTS = 3                    # [A] analistas concurrentes (base)

# Escalera de instancias: (nombre, clave de precio, RAM GB, vCPU sostenidas = linea base de creditos CPU)
RDS_LADDER = [
    ("db.t4g.micro", "rds_t4g_micro_hr", 1, 0.2),     # 2 vCPU, linea base 10 % por vCPU
    ("db.t4g.small", "rds_t4g_small_hr", 2, 0.4),     # 2 vCPU, 20 %
    ("db.t4g.medium", "rds_t4g_medium_hr", 4, 0.4),   # 2 vCPU, 20 %
    ("db.t4g.large", "rds_t4g_large_hr", 8, 0.6),     # 2 vCPU, 30 %
    ("db.r7g.large", "rds_r7g_large_hr", 16, 2.0),    # 2 vCPU sin creditos
    ("db.r7g.xlarge", "rds_r7g_xlarge_hr", 32, 4.0),
    ("db.r7g.2xlarge", "rds_r7g_2xlarge_hr", 64, 8.0),
]
RDS_STORAGE_KEY = "rds_gp2_gb_month"   # hoy gp2; gp3 cuesta lo mismo por GB en us-east-2 y da 3000 IOPS base

# Escenarios --------------------------------------------------------------------------------
INGEST_MULTIPLIERS = [1, 10, 100, 1000]
CONSUMPTION_SCENARIOS = [  # (nombre, actualizaciones/dia, analistas concurrentes)
    ("C1 minimo", 1, 1),
    ("C2 base", 8, 3),
    ("C3 medio", 48, 10),
    ("C4 alto", 96, 25),
    ("C5 muy alto", 288, 50),
]
STREAM_ALTERNATIVES = {
    "local": "Local (Docker), 0 en AWS",
    "msk_prov": "MSK aprovisionado + Glue Streaming",
    "msk_srv": "MSK Serverless + Glue Streaming",
    "kin_od": "Kinesis on-demand + Glue Streaming",
    "kin_prov": "Kinesis aprovisionado + Glue Streaming",
}

BASE = {k: v for k, v in dict(globals()).items()
        if k.isupper() and isinstance(v, (int, float, bool)) and not k.startswith("_")}


# =========================================================================================
# Precios
# =========================================================================================
def price(key):
    return PRICES[key]["usd"]


def tiered(key, qty):
    """Costo de `qty` unidades con precios por tramos (rangos begin/end de la API)."""
    rec = PRICES[key]
    if not rec.get("tiers"):
        return rec["usd"] * qty
    total, left = 0.0, qty
    for t in rec["tiers"]:
        lo = float(t["begin"] or 0)
        hi = float("inf") if t["end"] in (None, "Inf") else float(t["end"])
        part = max(0.0, min(qty, hi) - lo)
        total += part * t["usd"]
        left -= part
    return total


def free(a, amount):
    return amount if a["APPLY_FREE_TIER"] else 0


# =========================================================================================
# Modelo
# =========================================================================================
def stream_rows(eps, a):
    return eps + a["FX_ROWS_PER_S"]


def stream_bytes_per_day(eps, a):
    return (eps * a["TX_ROW_BYTES"] + a["FX_ROWS_PER_S"] * a["FX_ROW_BYTES"]) * 86400


def pick_rds(a, db_gb, cpu_vcpu):
    for name, key, ram, cpu in RDS_LADDER:
        if db_gb <= a["MEM_DB_RATIO"] * ram and cpu_vcpu <= a["CPU_UTIL_TARGET"] * cpu:
            return name, key, ram, cpu, True
    return RDS_LADDER[-1][0], RDS_LADDER[-1][1], RDS_LADDER[-1][2], RDS_LADDER[-1][3], False


def stream_aws(a, eps, mode):
    """Costo mensual del streaming si corre en AWS (sin RDS). Devuelve dict componente -> USD."""
    if mode == "local":
        return {}
    rows = stream_rows(eps, a)
    mbps = (eps + a["FX_ROWS_PER_S"]) * a["KAFKA_MSG_BYTES"] / 1e6
    gb_month = mbps * 86400 * DAYS / 1024
    dpu = max(a["GLUE_STREAM_MIN_DPU"], math.ceil(rows / a["GLUE_STREAM_ROWS_PER_DPU"]))
    hrs = HOURS * a["STREAM_UPTIME"]
    out = {"Streaming: Glue Streaming (DPU 24x7)": dpu * hrs * price("glue_dpu_hour") * a["GLUE_PRICE_MULT"],
           "Streaming: logs": a["STREAM_LOG_GB_MONTH"] * price("cw_logs_ingest_gb")}
    stored_gb = mbps * 86400 * a["KAFKA_RETENTION_DAYS"] / 1024
    if mode == "msk_prov":
        key = "msk_broker_t3_small_hr" if mbps <= a["MSK_SMALL_MAX_MBPS"] else "msk_broker_m7g_large_hr"
        out["Streaming: broker/cluster"] = a["MSK_BROKERS"] * hrs * price(key)
        out["Streaming: almacenamiento/datos"] = stored_gb * a["KAFKA_REPLICATION"] * price("msk_storage_gb_month")
    elif mode == "msk_srv":
        out["Streaming: broker/cluster"] = hrs * price("msk_serverless_cluster_hr") + \
            a["MSK_PARTITIONS"] * hrs * price("msk_serverless_partition_hr")
        out["Streaming: almacenamiento/datos"] = gb_month * (price("msk_serverless_in_gb") + price("msk_serverless_out_gb")) + \
            stored_gb * price("msk_serverless_storage_gb_month")
    elif mode == "kin_od":
        out["Streaming: broker/cluster"] = hrs * price("kinesis_od_stream_hr")
        out["Streaming: almacenamiento/datos"] = gb_month * (price("kinesis_od_in_gb") + price("kinesis_od_out_gb"))
    elif mode == "kin_prov":
        shards = max(1, math.ceil(mbps / a["KINESIS_SHARD_MBPS"]), math.ceil(rows / a["KINESIS_SHARD_RECS"]))
        out["Streaming: broker/cluster"] = shards * hrs * price("kinesis_prov_shard_hr")
        out["Streaming: almacenamiento/datos"] = rows * 86400 * DAYS * price("kinesis_prov_put_unit")
    return out


def model(a=None, batch_gb_day=None, stream_eps=None, refreshes=None, analysts=None,
          data_out_gb=None, stream_mode="local", rds_override=None):
    """Costo mensual por componente (USD) + diagnosticos."""
    a = {**BASE, **(a or {})}
    gb_day = BATCH_GB_DAY_TODAY if batch_gb_day is None else batch_gb_day
    eps = a["STREAM_EPS_TODAY"] if stream_eps is None else stream_eps
    refreshes = a["REFRESHES_PER_DAY"] if refreshes is None else refreshes
    analysts = a["ANALYSTS"] if analysts is None else analysts
    runs = a["RUNS_PER_DAY"] * DAYS
    gb_run = gb_day / a["RUNS_PER_DAY"]
    c = {}

    # Glue batch
    workers = min(a["GLUE_MAX_WORKERS"], max(a["GLUE_MIN_WORKERS"],
                  math.ceil(a["GLUE_VAR_DPU_MIN_PER_GB"] * gb_run / a["GLUE_TARGET_VAR_MIN"])))
    glue_min = a["GLUE_FIXED_MIN"] + a["GLUE_VAR_DPU_MIN_PER_GB"] * gb_run / workers
    c["Glue (batch)"] = runs * workers * max(glue_min, a["GLUE_MIN_BILLED_MIN"]) / 60 * price("glue_dpu_hour") * a["GLUE_PRICE_MULT"]

    # dbt en Fargate
    dbt_min = a["DBT_FIXED_MIN"] + a["DBT_MIN_PER_GB"] * gb_run
    task_h = runs * dbt_min / 60
    c["Fargate (dbt)"] = task_h * (a["FARGATE_VCPU"] * price("fargate_vcpu_hr") + a["FARGATE_GB"] * price("fargate_gb_hr"))

    # Almacenamiento en RDS y eleccion de instancia
    stream_gb_day = stream_bytes_per_day(eps, a) / 1024 ** 3
    stream_hot_gb = stream_gb_day * a["STREAM_HOT_DAYS"]
    db_gb = a["BASE_DB_GB"] + a["BATCH_DB_FACTOR"] * gb_run + stream_hot_gb
    storage_gb = max(a["MIN_STORAGE_GB"], math.ceil(db_gb * a["STORAGE_HEADROOM"]))
    cpu = (stream_rows(eps, a) * a["STREAM_CPU_S_PER_ROW"]
           + analysts * a["ANALYST_CPU"]
           + refreshes * a["DASH_QUERIES"] * a["DASH_QUERY_CPU_S"] / 86400
           + a["RUNS_PER_DAY"] * dbt_min * a["DBT_RDS_VCPU"] / 1440)
    if rds_override:
        name, key, ram, cap = next(x for x in RDS_LADDER if x[0] == rds_override)
        fits = True
    else:
        name, key, ram, cap, fits = pick_rds(a, db_gb, cpu)
    c["RDS instancia"] = HOURS * price(key) * a.get("RDS_PRICE_MULT", 1.0)
    c["RDS almacenamiento"] = storage_gb * price(RDS_STORAGE_KEY)

    # S3
    archive_gb = (stream_gb_day * a["STREAM_ARCHIVE_RATIO"] * a["STREAM_ARCHIVE_DAYS"]) if a["STREAM_ARCHIVE"] else 0
    s3_gb = gb_day * a["S3_RETENTION_DAYS"] + archive_gb
    c["S3"] = s3_gb * price("s3_standard_gb_month") + runs * (a["S3_REQUESTS_PER_RUN"] * price("s3_put_request")
                                                              + a["S3_GETS_PER_RUN"] * price("s3_get_request"))

    # Transferencia saliente
    out_gb = (refreshes * DAYS * a["DASH_PAYLOAD_GB"] + analysts * a["ADHOC_GB_PER_ANALYST"]) if data_out_gb is None else data_out_gb
    c["Transferencia saliente"] = tiered("data_transfer_out_gb", max(0.0, out_gb - free(a, a["FREE_DTO_GB"])))

    # Red: IPv4 publica de RDS (24x7) + de la tarea dbt mientras corre
    c["IPv4 publica"] = (HOURS + task_h) * price("public_ipv4_hr")

    # Otros: Step Functions, Secrets, ECR, CloudWatch, SNS, EventBridge
    other = 0.0
    other += max(0, runs * a["SFN_TRANSITIONS_PER_RUN"] - free(a, a["FREE_SFN_TRANSITIONS"])) * price("sfn_state_transition")
    other += a["SECRETS"] * price("secrets_manager_secret_month") + runs * a["SECRET_API_CALLS_PER_RUN"] * price("secrets_manager_api_call")
    other += a["ECR_GB"] * price("ecr_gb_month")
    log_gb = runs * (a["LOG_FIXED_MB_PER_RUN"] + a["LOG_MB_PER_GB"] * gb_run) / 1024
    other += max(0.0, log_gb - free(a, a["FREE_CW_LOGS_GB"])) * price("cw_logs_ingest_gb")
    other += log_gb * a["LOG_RETENTION_DAYS"] / DAYS * price("cw_logs_storage_gb_month")
    other += max(0, a["CW_ALARMS"] - free(a, a["FREE_CW_ALARMS"])) * price("cw_alarm_month")
    other += max(0, a["CW_DASHBOARDS"] - free(a, a["FREE_CW_DASHBOARDS"])) * price("cw_dashboard_month")
    other += max(0, a["CW_CUSTOM_METRICS"] - free(a, a["FREE_CW_METRICS"])) * price("cw_custom_metric_month")
    other += tiered("sns_email_notification", max(0, a["SNS_EMAILS_PER_MONTH"] - free(a, a["FREE_SNS_EMAILS"])))
    other += runs * a["EVENTS_PER_RUN"] * price("eventbridge_event")
    c["Otros (Step Functions, Secrets, ECR, CloudWatch, SNS)"] = other

    # Streaming en AWS (alternativas)
    for k, v in stream_aws(a, eps, stream_mode).items():
        c[k] = v

    return {"components": c, "total": sum(c.values()),
            "diag": dict(rds=name, rds_fits=fits, db_gb=db_gb, storage_gb=storage_gb, cpu_vcpu=cpu,
                         glue_workers=workers, glue_min=glue_min, dbt_min=dbt_min, wall_min=glue_min + dbt_min + 0.5,
                         stream_gb_day=stream_gb_day, out_gb=out_gb, s3_gb=s3_gb, gb_run=gb_run,
                         file_split=gb_run > a["FILE_SPLIT_LIMIT_GB"],
                         days_to_fill=days_to_fill(a, eps, gb_run))}


def days_to_fill(a, eps, gb_run, storage_gb=None):
    """Dias hasta llenar el disco SIN purga (lo que ocurre hoy), partiendo de la base y el batch."""
    storage_gb = storage_gb or a["MIN_STORAGE_GB"]
    usable = storage_gb / a["STORAGE_HEADROOM"] - a["BASE_DB_GB"] - a["BATCH_DB_FACTOR"] * gb_run
    per_day = stream_bytes_per_day(eps, a) / 1024 ** 3
    return math.inf if per_day <= 0 else max(0.0, usable / per_day)


# =========================================================================================
# Tablas y graficos
# =========================================================================================
GROUPS = [  # (etiqueta, componentes que agrupa)
    ("RDS instancia", ["RDS instancia"]),
    ("RDS almacenamiento", ["RDS almacenamiento"]),
    ("Glue batch", ["Glue (batch)"]),
    ("Fargate dbt", ["Fargate (dbt)"]),
    ("S3", ["S3"]),
    ("Transferencia saliente", ["Transferencia saliente"]),
    ("IPv4 publica", ["IPv4 publica"]),
    ("Otros", ["Otros (Step Functions, Secrets, ECR, CloudWatch, SNS)"]),
    ("Streaming en AWS", ["Streaming: Glue Streaming (DPU 24x7)", "Streaming: logs", "Streaming: broker/cluster",
                          "Streaming: almacenamiento/datos"]),
]
COLORS = ["#1f4e79", "#5b9bd5", "#e69f00", "#d55e00", "#009e73", "#cc79a7", "#7f7f7f", "#b3b3b3", "#7b3294"]


def grouped(res):
    return {lab: sum(res["components"].get(k, 0) for k in keys) for lab, keys in GROUPS}


def fmt(x):
    return f"{x:,.2f}"


def md_table(headers, rows):
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    out += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return "\n".join(out)


def ingest_args(k):
    return dict(batch_gb_day=BATCH_GB_DAY_TODAY * k, stream_eps=STREAM_EPS_TODAY * k)


def build():
    out_md, results = [], {}
    charts = HERE / "charts"
    charts.mkdir(exist_ok=True)

    # ---- T1: costo por componente por escenario de ingesta (streaming local)
    res = {k: model(**ingest_args(k)) for k in INGEST_MULTIPLIERS}
    labels = [lab for lab, _ in GROUPS if lab != "Streaming en AWS"]
    rows = []
    for lab in labels:
        rows.append([lab] + [fmt(grouped(res[k])[lab]) for k in INGEST_MULTIPLIERS])
    rows.append(["**Total (streaming local)**"] + [f"**{fmt(res[k]['total'])}**" for k in INGEST_MULTIPLIERS])
    hdr = ["Componente (USD/mes)"] + [f"{k}x ({BATCH_GB_DAY_TODAY*k:g} GB/dia; {STREAM_EPS_TODAY*k:g} ev/s)" for k in INGEST_MULTIPLIERS]
    out_md.append("### Tabla 1. Costo mensual por componente segun el volumen de ingesta (consumo base: "
                  f"{REFRESHES_PER_DAY} actualizaciones/dia, {ANALYSTS} analistas)\n\n" + md_table(hdr, rows))
    results["tabla1"] = {k: {"total": res[k]["total"], **grouped(res[k])} for k in INGEST_MULTIPLIERS}

    # ---- T2: puntos de quiebre (diagnosticos)
    rows = []
    for k in INGEST_MULTIPLIERS:
        d = res[k]["diag"]
        fill = d["days_to_fill"]
        rows.append([f"{k}x", d["rds"] + ("" if d["rds_fits"] else " (insuficiente)"), f"{d['db_gb']:.1f}", f"{d['storage_gb']}",
                     f"{d['stream_gb_day']:.2f}", ("> 1 ano" if fill > 365 else ("ya excede" if fill < 1 else f"{fill:.0f}")),
                     d["glue_workers"], f"{d['wall_min']:.0f}", "Si" if d["wall_min"] > BASE["DBT_SLA_MIN"] else "No",
                     "Si" if d["file_split"] else "No"])
    out_md.append("### Tabla 2. Puntos de quiebre de la arquitectura barata\n\n" + md_table(
        ["Ingesta", "Instancia RDS requerida", "Base (GB)", "Disco (GB)", "Streaming (GB/dia)", "Dias para llenar 20 GB sin purga",
         "Workers Glue", "Duracion corrida (min)", "Excede SLA 60 min", "Exige partir el JSON"], rows))
    results["tabla2"] = {k: {kk: (vv if vv != math.inf else None) for kk, vv in res[k]["diag"].items()} for k in INGEST_MULTIPLIERS}

    # ---- T3: streaming en AWS, alternativas
    alt = {k: {m: model(stream_mode=m, **ingest_args(k)) for m in STREAM_ALTERNATIVES} for k in INGEST_MULTIPLIERS}
    rows = []
    for m, nm in STREAM_ALTERNATIVES.items():
        rows.append([nm] + [fmt(alt[k][m]["total"]) for k in INGEST_MULTIPLIERS])
    out_md.append("### Tabla 3. Costo mensual total (USD) segun donde corre el streaming\n\n" + md_table(
        ["Arquitectura de streaming"] + [f"{k}x ({STREAM_EPS_TODAY*k:g} ev/s)" for k in INGEST_MULTIPLIERS], rows))
    rows = []
    for m, nm in STREAM_ALTERNATIVES.items():
        if m == "local":
            continue
        rows.append([nm] + [fmt(alt[k][m]["total"] - alt[k]["local"]["total"]) for k in INGEST_MULTIPLIERS])
    out_md.append("### Tabla 3b. Sobrecosto mensual (USD) de llevar el streaming a AWS (vs. local)\n\n" + md_table(
        ["Arquitectura de streaming"] + [f"{k}x" for k in INGEST_MULTIPLIERS], rows))
    results["tabla3"] = {k: {m: alt[k][m]["total"] for m in STREAM_ALTERNATIVES} for k in INGEST_MULTIPLIERS}

    # ---- T4: consumo (ingesta hoy y 100x)
    rows = []
    cons = {}
    for nm, r, an in CONSUMPTION_SCENARIOS:
        row = [nm, r, an]
        for k in (1, 100):
            m = model(refreshes=r, analysts=an, **ingest_args(k))
            cons[(nm, k)] = m
            row += [f"{m['diag']['out_gb']:.0f}", m["diag"]["rds"], fmt(m["components"]["Transferencia saliente"]), fmt(m["total"])]
        rows.append(row)
    out_md.append("### Tabla 4. Costo mensual segun el consumo (ingesta hoy 1x y 100x, streaming local)\n\n" + md_table(
        ["Escenario", "Actualiz./dia", "Analistas", "1x: GB salida/mes", "1x: RDS", "1x: salida USD", "1x: total USD",
         "100x: GB salida/mes", "100x: RDS", "100x: salida USD", "100x: total USD"], rows))
    results["tabla4"] = {f"{nm}|{k}": v["total"] for (nm, k), v in cons.items()}

    # ---- T5: umbrales de la instancia RDS por analistas (ingesta hoy)
    rows = []
    prev = None
    for an in range(1, 201):
        m = model(analysts=an, refreshes=REFRESHES_PER_DAY)
        if m["diag"]["rds"] != prev:
            rows.append([an, m["diag"]["rds"], f"{m['diag']['cpu_vcpu']:.2f}", fmt(m["components"]["RDS instancia"])])
            prev = m["diag"]["rds"]
    out_md.append("### Tabla 5. Analistas concurrentes que obligan a cambiar de instancia (ingesta 1x, 8 actualiz./dia)\n\n" + md_table(
        ["Desde N analistas", "Instancia RDS", "Carga CPU promedio (vCPU)", "RDS instancia USD/mes"], rows))
    results["tabla5"] = rows

    # ---- T6: crecimiento del disco por streaming
    rows = []
    for k in INGEST_MULTIPLIERS:
        eps = STREAM_EPS_TODAY * k
        gb_day = stream_bytes_per_day(eps, BASE) / 1024 ** 3
        fill = days_to_fill(BASE, eps, BATCH_GB_DAY_TODAY * k)
        rows.append([f"{eps:g}", f"{stream_rows(eps, BASE):,.1f}", f"{gb_day * 1024:,.0f}", f"{gb_day * DAYS:,.1f}",
                     "> 1 ano" if fill > 365 else ("ya excede (el batch ocupa el disco)" if fill < 1 else f"{fill:.0f}")])
    out_md.append("### Tabla 6. Crecimiento de RDS por streaming (sin purga)\n\n" + md_table(
        ["Eventos/s", "Filas/s", "MB/dia", "GB/mes", "Dias hasta llenar 20 GB"], rows))

    # ---- T7: duracion de corridas
    rows = []
    for k in INGEST_MULTIPLIERS:
        d = res[k]["diag"]
        rows.append([f"{k}x", f"{BATCH_GB_DAY_TODAY*k:g}", d["glue_workers"], f"{d['glue_min']:.1f}", f"{d['dbt_min']:.1f}",
                     f"{d['wall_min']:.0f}"])
    out_md.append("### Tabla 7. Duracion de una corrida batch segun el volumen\n\n" + md_table(
        ["Ingesta", "GB por corrida", "Workers Glue", "Glue (min)", "dbt (min)", "Total (min)"], rows))

    # ---- T10: elasticidad (cambio relativo del costo por cada x10 del factor)
    rows = []
    ks = INGEST_MULTIPLIERS
    for k0, k1 in zip(ks[:-1], ks[1:]):
        c0, c1 = res[k0]["total"], res[k1]["total"]
        rows.append([f"Ingesta {k0}x -> {k1}x (batch y streaming, consumo base)", fmt(c0), fmt(c1), f"x{c1 / c0:.2f}",
                     f"{math.log(c1 / c0) / math.log(10):.2f}"])
    for k in (1, 100):
        c0 = model(**ingest_args(k))["total"]
        c1 = model(refreshes=REFRESHES_PER_DAY * 10, analysts=ANALYSTS * 10, **ingest_args(k))["total"]
        rows.append([f"Consumo x10 ({REFRESHES_PER_DAY}->{REFRESHES_PER_DAY*10} actualiz./dia, {ANALYSTS}->{ANALYSTS*10} analistas), ingesta {k}x",
                     fmt(c0), fmt(c1), f"x{c1 / c0:.2f}", f"{math.log(c1 / c0) / math.log(10):.2f}"])
    for k in (1, 100):
        c0 = model(**ingest_args(k))["total"]
        c1 = model(refreshes=REFRESHES_PER_DAY * 10, **ingest_args(k))["total"]
        rows.append([f"Solo actualizaciones del tablero x10 ({REFRESHES_PER_DAY}->{REFRESHES_PER_DAY*10}/dia), ingesta {k}x",
                     fmt(c0), fmt(c1), f"x{c1 / c0:.2f}", f"{math.log(c1 / c0) / math.log(10):.2f}"])
    out_md.append("### Tabla 8. Elasticidad: cuanto crece el costo cuando un factor se multiplica por 10\n\n" + md_table(
        ["Cambio", "Costo antes (USD/mes)", "Costo despues (USD/mes)", "Factor", "Elasticidad (log10)"], rows))
    results["tabla10"] = rows

    # ---- T9: modelo vs Cost Explorer
    ce_path = HERE / "cost_explorer.json"
    if ce_path.exists():
        ce = json.loads(ce_path.read_text(encoding="utf-8"))
        s = ce["by_service"]
        cal = calibration()["components"]
        otros_ce = sum(v for k, v in s.items() if k in ("AWS Secrets Manager", "Amazon EC2 Container Registry (ECR)", "AmazonCloudWatch",
                                                         "AWS Step Functions", "Amazon Simple Notification Service",
                                                         "CloudWatch Events", "Amazon Simple Queue Service",
                                                         "AWS CloudFormation", "AWS Key Management Service"))
        pairs = [
            ("RDS (instancia + disco)", cal["RDS instancia"] + cal["RDS almacenamiento"], s.get("Amazon Relational Database Service", 0)),
            ("IPv4 publica (VPC)", cal["IPv4 publica"], s.get("Amazon Virtual Private Cloud", 0)),
            ("Glue", cal["Glue (batch)"], s.get("AWS Glue", 0)),
            ("Fargate (ECS)", cal["Fargate (dbt)"], s.get("Amazon Elastic Container Service", 0)),
            ("S3", cal["S3"], s.get("Amazon Simple Storage Service", 0)),
            ("Otros (Secrets, ECR, CloudWatch, etc.)", cal["Otros (Step Functions, Secrets, ECR, CloudWatch, SNS)"], otros_ce),
        ]
        rows = [[n, fmt(m), fmt(r), f"{(m - r):+.2f}"] for n, m, r in pairs]
        tm, tr = sum(p[1] for p in pairs), sum(p[2] for p in pairs)
        rows.append(["**Total**", f"**{fmt(tm)}**", f"**{fmt(tr)}**", f"**{tm - tr:+.2f}**"])
        out_md.append(f"### Tabla 9. Modelo (escenario observado: 4.5 corridas en 30 dias) vs. Cost Explorer, costo bruto de uso "
                      f"{ce['period']['start']} a {ce['period']['end']} (USD)\n\n" + md_table(
                          ["Servicio", "Modelo", "Cost Explorer", "Diferencia"], rows))
        results["tabla9"] = {n: {"modelo": m, "cost_explorer": r} for n, m, r in pairs}

    # ================= Graficos =================
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 110})

    # (a) apilado por componente
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5.2))
    x = range(len(INGEST_MULTIPLIERS))
    bottom = [0] * len(INGEST_MULTIPLIERS)
    bottom_p = [0] * len(INGEST_MULTIPLIERS)
    for (lab, _), col in zip(GROUPS, COLORS):
        if lab == "Streaming en AWS":
            continue
        v = [grouped(res[k])[lab] for k in INGEST_MULTIPLIERS]
        tot = [res[k]["total"] for k in INGEST_MULTIPLIERS]
        ax1.bar(x, v, bottom=bottom, color=col, label=lab, width=0.6)
        ax2.bar(x, [vi / t * 100 for vi, t in zip(v, tot)], bottom=bottom_p, color=col, width=0.6)
        bottom = [b + vi for b, vi in zip(bottom, v)]
        bottom_p = [b + vi / t * 100 for b, vi, t in zip(bottom_p, v, tot)]
    for xi, t in zip(x, bottom):
        ax1.text(xi, t, f"USD {t:,.0f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
    names = [f"{k}x\n{BATCH_GB_DAY_TODAY*k:g} GB/dia batch\n{STREAM_EPS_TODAY*k:g} ev/s" for k in INGEST_MULTIPLIERS]
    for ax in (ax1, ax2):
        ax.set_xticks(list(x))
        ax.set_xticklabels(names)
    ax1.set_ylabel("USD por mes")
    ax1.set_title("Costo mensual por componente (streaming local)")
    ax1.set_ylim(0, max(bottom) * 1.12)
    ax2.set_ylabel("% del costo mensual")
    ax2.set_title("Participacion de cada componente")
    ax1.legend(fontsize=8, loc="upper left", frameon=False)
    fig.suptitle("Figura 1. Costo mensual segun el volumen de ingesta (consumo base)", fontweight="bold")
    fig.tight_layout()
    fig.savefig(charts / "fig1_costo_por_componente.png")
    plt.close(fig)

    # (a2) streaming en AWS
    fig, ax = plt.subplots(figsize=(11, 5.2))
    w = 0.16
    cols = ["#7f7f7f", "#1f4e79", "#5b9bd5", "#e69f00", "#d55e00"]
    for i, (m, nm) in enumerate(STREAM_ALTERNATIVES.items()):
        v = [alt[k][m]["total"] for k in INGEST_MULTIPLIERS]
        bars = ax.bar([xi + (i - 2) * w for xi in x], v, w, color=cols[i], label=nm)
        for b_, vi in zip(bars, v):
            ax.text(b_.get_x() + b_.get_width() / 2, vi * 1.05, f"{vi:,.0f}", ha="center", fontsize=7, rotation=90)
    ax.set_yscale("log")
    ax.set_ylim(10, 2.5e4)
    ax.set_xticks(list(x))
    ax.set_xticklabels([f"{k}x ({STREAM_EPS_TODAY*k:g} ev/s)" for k in INGEST_MULTIPLIERS])
    ax.set_ylabel("USD por mes (escala logaritmica)")
    ax.set_title("Figura 2. Costo mensual total segun donde corre el streaming", fontweight="bold")
    ax.legend(fontsize=8, frameon=False, loc="upper left")
    fig.tight_layout()
    fig.savefig(charts / "fig2_streaming_en_aws.png")
    plt.close(fig)

    # (b) costo vs consumo
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))
    gbs = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000]
    for k, col in ((1, "#1f4e79"), (100, "#d55e00")):
        ax1.plot(gbs, [model(data_out_gb=g, **ingest_args(k))["total"] for g in gbs], "-o", color=col, label=f"Ingesta {k}x")
    ax1.axvline(FREE_DTO_GB, color="#7f7f7f", ls="--", lw=1)
    ax1.text(FREE_DTO_GB * 1.05, ax1.get_ylim()[0], " 100 GB gratis", fontsize=8, color="#555", va="bottom")
    ax1.set_xscale("log")
    ax1.set_xlabel("Datos que salen a Internet (GB por mes)")
    ax1.set_ylabel("USD por mes")
    ax1.set_title("Costo vs. datos transferidos (Power BI + SQL ad hoc)")
    ax1.legend(frameon=False)
    ans = list(range(1, 101))
    for k, col in ((1, "#1f4e79"), (100, "#d55e00")):
        ax2.step(ans, [model(analysts=a_, **ingest_args(k))["total"] for a_ in ans], where="post", color=col, label=f"Ingesta {k}x")
    prev = None
    for a_ in ans:
        r_ = model(analysts=a_)["diag"]["rds"]
        if r_ != prev and a_ > 1:
            ax2.axvline(a_, color="#bbbbbb", ls=":", lw=1)
            ax2.text(a_, ax2.get_ylim()[1] * 0.02, f" {r_}", rotation=90, fontsize=7, va="bottom", color="#555")
        prev = r_
    ax2.set_xlabel("Analistas concurrentes")
    ax2.set_ylabel("USD por mes")
    ax2.set_title("Costo vs. analistas (saltos = cambio de instancia RDS)")
    ax2.legend(frameon=False)
    fig.suptitle("Figura 3. Costo mensual segun el consumo", fontweight="bold")
    fig.tight_layout()
    fig.savefig(charts / "fig3_costo_vs_consumo.png")
    plt.close(fig)

    # (c) tornado
    drivers = [
        ("Volumen batch (GB/dia)", "gb"), ("Eventos/s de streaming", "eps"), ("Corridas batch por dia", "RUNS_PER_DAY"),
        ("Precio de la instancia RDS", "RDS_PRICE_MULT"), ("Minutos fijos de Glue", "GLUE_FIXED_MIN"),
        ("Trabajo de Glue por GB", "GLUE_VAR_DPU_MIN_PER_GB"), ("Minutos dbt por GB", "DBT_MIN_PER_GB"),
        ("Retencion de streaming en RDS (dias)", "STREAM_HOT_DAYS"), ("Bytes por fila en RDS", "ROW_BYTES"),
        ("Retencion de JSON en S3 (dias)", "S3_RETENTION_DAYS"), ("Actualizaciones de tablero/dia", "refreshes"),
        ("Analistas concurrentes", "analysts"), ("GB por actualizacion del tablero", "DASH_PAYLOAD_GB"),
        ("CPU por analista (vCPU)", "ANALYST_CPU"), ("CPU por fila de streaming", "STREAM_CPU_S_PER_ROW"),
        ("Filas/s por DPU de Glue Streaming", "GLUE_STREAM_ROWS_PER_DPU"), ("Precio del DPU de Glue", "GLUE_PRICE_MULT"),
        ("Horas/mes que corre el streaming en AWS", "STREAM_UPTIME"),
    ]

    def run_with(sc, key, mult):
        k, mode, refreshes, analysts = sc
        kw = dict(ingest_args(k), refreshes=refreshes, analysts=analysts)
        a = {}
        if key == "gb":
            kw["batch_gb_day"] *= mult
        elif key == "eps":
            kw["stream_eps"] *= mult
        elif key == "refreshes":
            kw["refreshes"] = refreshes * mult
        elif key == "analysts":
            kw["analysts"] = analysts * mult
        elif key == "ROW_BYTES":
            a = {"TX_ROW_BYTES": BASE["TX_ROW_BYTES"] * mult, "FX_ROW_BYTES": BASE["FX_ROW_BYTES"] * mult}
        elif key == "RDS_PRICE_MULT":
            a = {"RDS_PRICE_MULT": mult}
        else:
            a = {key: BASE[key] * mult}
        return model(a=a, stream_mode=mode, **kw)["total"]

    tor = {}
    fig, axes = plt.subplots(1, 3, figsize=(21, 6.6))
    scen = (("A. Hoy: ingesta 1x, streaming local, consumo base", (1, "local", REFRESHES_PER_DAY, ANALYSTS)),
            ("B. Ingesta 100x con Kinesis on-demand + Glue, consumo base", (100, "kin_od", REFRESHES_PER_DAY, ANALYSTS)),
            ("C. Ingesta 10x, streaming local, consumo alto (96 actualiz./dia, 25 analistas)", (10, "local", 96, 25)))
    for ax, (title, sc) in zip(axes, scen):
        k, mode, refreshes, analysts = sc
        base = model(stream_mode=mode, refreshes=refreshes, analysts=analysts, **ingest_args(k))["total"]
        data = []
        for lab, key in drivers:
            lo, hi = run_with(sc, key, 0.5), run_with(sc, key, 1.5)
            data.append((lab, lo - base, hi - base))
        data.sort(key=lambda t: max(abs(t[1]), abs(t[2])))
        full = data
        data = data[-10:]  # los 10 supuestos de mayor efecto
        tor[title] = {"base": base, "drivers": {lab: [lo, hi] for lab, lo, hi in full}}
        for i, (lab, lo, hi) in enumerate(data):
            ax.barh(i, lo, color="#5b9bd5", height=0.6, label="-50 %" if i == 0 else None)
            ax.barh(i, hi, color="#d55e00", height=0.6, label="+50 %" if i == 0 else None)
        ax.set_yticks(range(len(data)))
        ax.set_yticklabels([d[0] for d in data], fontsize=8)
        ax.axvline(0, color="black", lw=0.8)
        ax.set_xlabel(f"Cambio del costo mensual (USD) respecto de USD {base:,.0f}")
        ax.set_title("\n".join(__import__("textwrap").wrap(title, 55)), fontsize=9)
        ax.legend(frameon=False, loc="lower right")
    fig.suptitle("Figura 4. Sensibilidad del costo mensual a +/-50 % en cada supuesto", fontweight="bold")
    fig.tight_layout()
    fig.savefig(charts / "fig4_tornado.png")
    plt.close(fig)
    results["tornado"] = tor
    rows = []
    for title, t in tor.items():
        top = sorted(t["drivers"].items(), key=lambda kv: -max(abs(kv[1][0]), abs(kv[1][1])))[:6]
        for lab, (lo, hi) in top:
            rows.append([title[:2] + f" (base USD {t['base']:,.2f})", lab, f"{lo:+,.2f}", f"{hi:+,.2f}",
                         f"{abs(hi - lo) / 2 / t['base'] * 100:.0f} %"])
    out_md.append("### Tabla 10. Sensibilidad (+/-50 %): 6 supuestos con mayor efecto por escenario (cambio del costo mensual, USD)\n\n" + md_table(
        ["Escenario", "Supuesto", "-50 %", "+50 %", "Efecto medio / base"], rows))

    # (d) crecimiento del disco
    fig, ax = plt.subplots(figsize=(9, 5))
    days = list(range(0, 366))
    for k, col in zip(INGEST_MULTIPLIERS, ["#1f4e79", "#5b9bd5", "#e69f00", "#d55e00"]):
        eps = STREAM_EPS_TODAY * k
        per_day = stream_bytes_per_day(eps, BASE) / 1024 ** 3
        ax.plot(days, [BASE["BASE_DB_GB"] + BASE["BATCH_DB_FACTOR"] * BATCH_GB_DAY_TODAY * k + per_day * d for d in days],
                color=col, label=f"{eps:g} ev/s ({per_day * 1024:,.0f} MB/dia)")
    ax.axhline(MIN_STORAGE_GB / STORAGE_HEADROOM, color="black", ls="--", lw=1)
    ax.text(250, MIN_STORAGE_GB / STORAGE_HEADROOM * 1.15, "Limite util de 20 GB (con 25 % de holgura)", fontsize=8)
    ax.set_yscale("log")
    ax.set_xlabel("Dias de operacion sin purga")
    ax.set_ylabel("Tamano de la base (GB, escala logaritmica)")
    ax.set_title("Figura 5. Crecimiento de RDS por streaming y cuando se llena el disco", fontweight="bold")
    ax.legend(frameon=False)
    fig.tight_layout()
    fig.savefig(charts / "fig5_crecimiento_disco.png")
    plt.close(fig)

    (HERE / "tablas.md").write_text("\n\n".join(out_md) + "\n", encoding="utf-8")
    (HERE / "resultados.json").write_text(json.dumps(results, indent=2, default=str, ensure_ascii=False), encoding="utf-8")
    return res, alt


def calibration():
    """Escenario 'hoy' para comparar con Cost Explorer: ~4.5 corridas en 30 dias, sin purga ni archivo."""
    runs_day = 4.5 / DAYS
    a = dict(RUNS_PER_DAY=runs_day, STREAM_ARCHIVE=False, STREAM_HOT_DAYS=0)
    return model(a=a, batch_gb_day=BATCH_GB_DAY_TODAY * runs_day)  # cada corrida procesa 0.05 GB


if __name__ == "__main__":
    res, alt = build()
    print((HERE / "tablas.md").read_text(encoding="utf-8"))
    cal = calibration()
    print("Calibracion (4.5 corridas/30 d):", {k: round(v, 3) for k, v in cal["components"].items()}, round(cal["total"], 2))
    print("Hoy (1 corrida/dia):", round(res[1]["total"], 2))
