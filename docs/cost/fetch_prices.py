"""Descarga precios on-demand de us-east-2 con la AWS Price List API y escribe prices.json.

Solo llamadas de lectura (pricing:GetProducts). Las credenciales se leen de .envs/aws.env
(ver aws_cli.py) y nunca se imprimen. Un precio que no se pueda obtener se completa con un
valor manual marcado con "source": "manual".

Uso:  python fetch_prices.py
"""
import json
from datetime import date
from pathlib import Path

from aws_cli import od_prices, products

R = "us-east-2"
U = "USE2-"
RDS = {"regionCode": R, "databaseEngine": "PostgreSQL", "deploymentOption": "Single-AZ"}

# clave -> (service_code, filtros, descripcion)
QUERIES = {
    "rds_t4g_micro_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.t4g.micro"}, "RDS PostgreSQL db.t4g.micro Single-AZ, USD/hora"),
    "rds_t4g_small_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.t4g.small"}, "RDS PostgreSQL db.t4g.small Single-AZ, USD/hora"),
    "rds_t4g_medium_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.t4g.medium"}, "RDS PostgreSQL db.t4g.medium Single-AZ, USD/hora"),
    "rds_t4g_large_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.t4g.large"}, "RDS PostgreSQL db.t4g.large Single-AZ, USD/hora"),
    "rds_r7g_large_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.r7g.large"}, "RDS PostgreSQL db.r7g.large Single-AZ, USD/hora (escalon superior, no burstable)"),
    "rds_r7g_xlarge_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.r7g.xl"}, "RDS PostgreSQL db.r7g.xlarge Single-AZ, USD/hora"),
    "rds_r7g_2xlarge_hr": ("AmazonRDS", {**RDS, "usagetype": U + "InstanceUsage:db.r7g.2xl"}, "RDS PostgreSQL db.r7g.2xlarge Single-AZ, USD/hora"),
    "rds_gp2_gb_month": ("AmazonRDS", {**RDS, "usagetype": U + "RDS:GP2-Storage"}, "RDS almacenamiento gp2, USD/GB-mes"),
    "rds_gp3_gb_month": ("AmazonRDS", {**RDS, "usagetype": U + "RDS:GP3-Storage"}, "RDS almacenamiento gp3, USD/GB-mes"),
    "glue_dpu_hour": ("AWSGlue", {"regionCode": R, "usagetype": U + "ETL-DPU-Hour"}, "Glue ETL (Glue 5.0 y anteriores, tambien streaming), USD/DPU-hora"),
    "fargate_vcpu_hr": ("AmazonECS", {"regionCode": R, "usagetype": U + "Fargate-vCPU-Hours:perCPU"}, "Fargate Linux/x86 vCPU, USD/hora"),
    "fargate_gb_hr": ("AmazonECS", {"regionCode": R, "usagetype": U + "Fargate-GB-Hours"}, "Fargate Linux/x86 memoria, USD/GB-hora"),
    "fargate_arm_vcpu_hr": ("AmazonECS", {"regionCode": R, "usagetype": U + "Fargate-ARM-vCPU-Hours:perCPU"}, "Fargate ARM vCPU, USD/hora (referencia)"),
    "s3_standard_gb_month": ("AmazonS3", {"regionCode": R, "productFamily": "Storage", "storageClass": "General Purpose", "usagetype": U + "TimedStorage-ByteHrs"}, "S3 Standard, USD/GB-mes (primeros 50 TB)"),
    "s3_put_request": ("AmazonS3", {"regionCode": R, "productFamily": "API Request", "usagetype": U + "Requests-Tier1"}, "S3 PUT/COPY/POST/LIST, USD/solicitud"),
    "s3_get_request": ("AmazonS3", {"regionCode": R, "productFamily": "API Request", "usagetype": U + "Requests-Tier2"}, "S3 GET/SELECT, USD/solicitud"),
    "sfn_state_transition": ("AmazonStates", {"regionCode": R, "usagetype": U + "StateTransition"}, "Step Functions Standard, USD/transicion de estado"),
    "data_transfer_out_gb": ("AWSDataTransfer", {"fromRegionCode": R, "transferType": "AWS Outbound", "usagetype": U + "DataTransfer-Out-Bytes"}, "Transferencia saliente a Internet, USD/GB por tramos (los 100 GB/mes iniciales son gratis en la capa gratuita permanente)"),
    "cw_custom_metric_month": ("AmazonCloudWatch", {"regionCode": R, "usagetype": U + "CW:MetricMonitorUsage"}, "CloudWatch metrica personalizada, USD/metrica-mes (por tramos)"),
    "cw_alarm_month": ("AmazonCloudWatch", {"regionCode": R, "usagetype": U + "CW:AlarmMonitorUsage"}, "CloudWatch alarma estandar, USD/alarma-mes"),
    "cw_dashboard_month": ("AmazonCloudWatch", {"usagetype": "DashboardsUsageHour-Basic"}, "CloudWatch dashboard personalizado, USD/dashboard-mes"),
    "cw_logs_ingest_gb": ("AmazonCloudWatch", {"regionCode": R, "productFamily": "Data Payload", "usagetype": U + "DataProcessing-Bytes"}, "CloudWatch Logs ingesta (Standard), USD/GB"),
    "cw_logs_storage_gb_month": ("AmazonCloudWatch", {"regionCode": R, "usagetype": U + "TimedStorage-ByteHrs"}, "CloudWatch Logs almacenamiento, USD/GB-mes"),
    "secrets_manager_secret_month": ("AWSSecretsManager", {"regionCode": R, "usagetype": U + "AWSSecretsManager-Secrets"}, "Secrets Manager, USD/secreto-mes"),
    "secrets_manager_api_call": ("AWSSecretsManager", {"regionCode": R, "usagetype": U + "AWSSecretsManagerAPIRequest"}, "Secrets Manager, USD/llamada API"),
    "public_ipv4_hr": ("AmazonVPC", {"regionCode": R, "usagetype": U + "PublicIPv4:InUseAddress"}, "IPv4 publica en uso, USD/hora"),
    "ecr_gb_month": ("AmazonECR", {"regionCode": R, "usagetype": U + "TimedStorage-ByteHrs"}, "ECR almacenamiento, USD/GB-mes"),
    "sns_email_notification": ("AmazonSNS", {"regionCode": R, "usagetype": U + "DeliveryAttempts-SMTP"}, "SNS entrega por email, USD/notificacion (primeras 1000/mes gratis)"),
    "eventbridge_event": ("AWSEvents", {"regionCode": R, "usagetype": U + "Event-64K-Chunks"}, "EventBridge eventos personalizados, USD/evento de 64 KB"),
    # Streaming en AWS: alternativas
    "msk_broker_t3_small_hr": ("AmazonMSK", {"regionCode": R, "usagetype": U + "Kafka.t3.small"}, "MSK aprovisionado kafka.t3.small, USD/broker-hora"),
    "msk_broker_m7g_large_hr": ("AmazonMSK", {"regionCode": R, "usagetype": U + "Kafka.m7g.large"}, "MSK aprovisionado kafka.m7g.large, USD/broker-hora"),
    "msk_storage_gb_month": ("AmazonMSK", {"regionCode": R, "usagetype": U + "Kafka.Storage.GP2"}, "MSK almacenamiento, USD/GB-mes"),
    "msk_serverless_cluster_hr": ("AmazonMSK", {"regionCode": R, "usagetype": U + "KafkaServerless-ClusterHours"}, "MSK Serverless, USD/cluster-hora"),
    "msk_serverless_partition_hr": ("AmazonMSK", {"regionCode": R, "usagetype": U + "KafkaServerless-PartitionHours"}, "MSK Serverless, USD/particion-hora"),
    "msk_serverless_in_gb": ("AmazonMSK", {"regionCode": R, "usagetype": U + "KafkaServerless-In-Bytes"}, "MSK Serverless datos entrantes, USD/GB"),
    "msk_serverless_out_gb": ("AmazonMSK", {"regionCode": R, "usagetype": U + "KafkaServerless-Out-Bytes"}, "MSK Serverless datos salientes, USD/GB"),
    "msk_serverless_storage_gb_month": ("AmazonMSK", {"regionCode": R, "usagetype": U + "KafkaServerless-StorageHours"}, "MSK Serverless almacenamiento, USD/GB-mes"),
    "kinesis_od_stream_hr": ("AmazonKinesis", {"regionCode": R, "usagetype": U + "OnDemand-StreamHour"}, "Kinesis on-demand Standard, USD/stream-hora"),
    "kinesis_od_in_gb": ("AmazonKinesis", {"regionCode": R, "usagetype": U + "OnDemand-BilledIncomingBytes"}, "Kinesis on-demand datos entrantes, USD/GB"),
    "kinesis_od_out_gb": ("AmazonKinesis", {"regionCode": R, "usagetype": U + "OnDemand-BilledOutgoingBytes"}, "Kinesis on-demand datos salientes, USD/GB"),
    "kinesis_prov_shard_hr": ("AmazonKinesis", {"regionCode": R, "usagetype": U + "Storage-ShardHour"}, "Kinesis aprovisionado, USD/shard-hora"),
    "kinesis_prov_put_unit": ("AmazonKinesis", {"regionCode": R, "usagetype": U + "PutRequestPayloadUnits"}, "Kinesis aprovisionado PUT payload unit (25 KB), USD/unidad"),
}

# Precios de respaldo (pagina oficial de precios) si la API no devuelve el producto.
MANUAL = {
    "cw_logs_storage_gb_month": (0.03, "GB-Mo", "CloudWatch Logs almacenamiento (pagina oficial)"),
}


def fetch(key, service, filters, desc):
    ps = products(service, filters)
    seen, tiers = set(), []
    for p in ps:
        for unit, d, usd, b, e in od_prices(p):
            t = (unit, usd, b, e)
            if t not in seen:
                seen.add(t)
                tiers.append({"unit": unit, "usd": usd, "begin": b, "end": e, "description": d})
    if not tiers:
        return None
    tiers.sort(key=lambda t: float(t["begin"] or 0))
    return {"description": desc, "service_code": service, "source": "aws-price-list-api",
            "usagetype": filters.get("usagetype"), "usd": tiers[0]["usd"], "unit": tiers[0]["unit"],
            "tiers": tiers if len(tiers) > 1 else None}


def main():
    out = {"_meta": {"region": R, "currency": "USD", "pricing_model": "On-Demand",
                     "fetched": date.today().isoformat(),
                     "api": "aws pricing get-products --region us-east-1"}}
    for key, (svc, flt, desc) in QUERIES.items():
        try:
            rec = fetch(key, svc, flt, desc)
        except Exception as exc:  # noqa: BLE001
            rec = None
            print(f"WARN {key}: {exc}")
        if rec is None and key in MANUAL:
            usd, unit, d = MANUAL[key]
            rec = {"description": d, "service_code": svc, "source": "manual", "usd": usd, "unit": unit, "tiers": None}
        if rec is None:
            print(f"MISSING {key}")
            continue
        out[key] = rec
        print(f"{key:36s} {rec['usd']:<12g} {rec['unit']:<10s} {rec['source']}")
    Path(__file__).with_name("prices.json").write_text(json.dumps(out, indent=2, ensure_ascii=False), encoding="utf-8")


if __name__ == "__main__":
    main()
