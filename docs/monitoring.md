# Monitoreo y alertas

El monitoreo está definido como código en el stack de Serverless
`infrastructure/serverless/monitoring/` (CloudFormation `payload-analytics-monitoring`):
un dashboard de CloudWatch y cuatro alarmas que notifican al tema SNS de alertas
que ya existía (`payload-analytics-alerts-dev`, leído de SSM
`/payload-analytics-alerts/PipelineAlertsTopicArn`). El streaming, que corre en
Docker en local, publica sus propias métricas en CloudWatch.

## Qué se monitorea

| Componente | Métricas | Origen |
| --- | --- | --- |
| Pipeline batch (Step Functions) | `ExecutionsStarted`, `ExecutionsSucceeded`, `ExecutionsFailed`, `ExecutionTime` de la state machine `payload-analytics-backend-orchestration` | `AWS/States`, automáticas |
| Glue y dbt | `ServiceIntegrationsSucceeded` / `ServiceIntegrationsFailed` de `glue:startJobRun.sync` (Glue) y `ecs:runTask.sync` (dbt en Fargate) | `AWS/States`, automáticas. Así no hace falta activar las métricas de job de Glue (cuestan y alargan el arranque) |
| RDS `payload-analytics-rds` | `CPUUtilization`, `FreeStorageSpace`, `DatabaseConnections` | `AWS/RDS`, automáticas |
| Streaming (Spark) | `TransactionsWritten`, `FxTicksWritten` (suma por minuto), `TransactionLatencyP95Seconds`, `StreamLagSeconds` | `PayloadAnalytics/Streaming`, publicadas por `streaming/src/spark/stream_job.py` |

### Métricas del streaming

`stream_job.py` acumula en memoria lo que escribe cada micro-batch y un hilo
aparte llama a `put_metric_data` una vez cada `METRICS_INTERVAL_SECONDS`
(60 s por defecto), no en cada micro-batch, para que el costo de API sea mínimo
(unas 43 000 llamadas al mes, dentro del free tier de 1 millón).

- `TransactionsWritten` / `FxTicksWritten`: filas escritas en
  `silver.stream_transactions` y `silver.stream_fx_rates` en el intervalo.
- `TransactionLatencyP95Seconds`: percentil 95 de `processed_at - event_time` de
  las transacciones del intervalo (la misma latencia que se mide en la tabla).
- `StreamLagSeconds`: hora actual menos el `event_time` más reciente escrito en
  cualquiera de las dos tablas. Se publica aunque no lleguen filas, así que si
  los productores o el job se atascan, el lag crece y la alarma salta.

Si CloudWatch falla (sin red, sin permisos), el job solo escribe un `WARNING`
en el log y sigue procesando; las métricas nunca detienen el stream.
`METRICS_ENABLED=false` en `streaming/.envs/config.env` las desactiva.

## Alarmas

Todas envían correo por SNS al pasar a `ALARM` y al volver a `OK`.

| Alarma | Condición | Por qué |
| --- | --- | --- |
| `payload-analytics-monitoring-pipeline-failed` | `ExecutionsFailed` ≥ 1 (suma en 5 min, 1 periodo) | Un fallo en Glue, dbt (incluye un test de dbt fallido) o el archivado deja el archivo en `failed/` y Gold sin actualizar. Sin ejecuciones no hay datos: se trata como `notBreaching` |
| `payload-analytics-monitoring-rds-low-storage` | `FreeStorageSpace` mínimo < 5 GB (5 min) | La instancia tiene 20 GB y las tablas de ticks del streaming crecen sin parar. 5 GB da margen para limpiar o ampliar antes de que Postgres se quede sin disco |
| `payload-analytics-monitoring-rds-high-cpu` | `CPUUtilization` promedio > 80 % durante 10 min (2 periodos de 5 min) | Una db.t4g.micro con CPU alta de forma sostenida hace lentos dbt y Power BI y puede agotar los créditos de CPU. Dos periodos evitan avisos por picos cortos (por ejemplo un `dbt run`) |
| `payload-analytics-monitoring-streaming-lag` | `StreamLagSeconds` máximo > 60 s durante 3 periodos de 1 min | El streaming normal escribe con pocos segundos de retraso. Más de un minuto sostenido significa que el job o los productores están detenidos o atrasados |

**Por qué la alarma de lag usa `TreatMissingData: notBreaching`.** El streaming
corre en Docker en un portátil y se apaga a propósito la mayor parte del tiempo.
Cuando el contenedor de Spark está apagado no se publica ninguna métrica; con el
valor por defecto la alarma quedaría en `INSUFFICIENT_DATA` y con `breaching`
mandaría un correo cada vez que alguien apaga Docker. Con `notBreaching` la
ausencia de datos se interpreta como "todo bien" y la alarma solo avisa cuando
el job está encendido y realmente atrasado. La contrapartida es que un job que
se cae del todo no avisa por esta alarma; eso se ve en el dashboard (el panel
de filas escritas queda vacío).

Costo: 4 alarmas estándar × USD 0.10 = USD 0.40/mes; un dashboard (los tres
primeros de la cuenta son gratis); 4 métricas personalizadas (gratis dentro de
las 10 del free tier, si no USD 0.30 c/u).

## Cómo abrir el dashboard

Consola de AWS → CloudWatch → Dashboards → `payload-analytics`, o directo:

<https://us-east-2.console.aws.amazon.com/cloudwatch/home?region=us-east-2#dashboards/dashboard/payload-analytics>

Filas, de arriba abajo: texto de ayuda y estado de las alarmas; pipeline batch
(ejecuciones, duración, Glue y dbt); RDS (CPU, espacio libre, conexiones);
streaming (filas por minuto, latencia p95 y lag). Por defecto muestra las
últimas 6 horas.

## Cómo llegan las alertas

CloudWatch publica en el tema SNS `payload-analytics-alerts-dev`, que tiene una
suscripción de correo (la variable `ALERTS_EMAIL` del stack `alerts`). El correo
llega con asunto `ALARM: "payload-analytics-monitoring-..." in US East (Ohio)`
e incluye el motivo, el umbral y el valor. Al volver a la normalidad llega otro
con `OK`. Es el mismo tema que usa la state machine para su aviso de fallo, así
que un fallo del pipeline genera dos correos: el de la state machine (con el
error) y el de la alarma. Al crear el stack cada alarma pasa de
`INSUFFICIENT_DATA` a `OK` y manda un correo `OK`; es normal.

## Probar una alerta (demo)

Fuerza la alarma del pipeline a `ALARM` sin tocar nada del pipeline:

```bash
aws cloudwatch set-alarm-state \
  --alarm-name payload-analytics-monitoring-pipeline-failed \
  --state-value ALARM \
  --state-reason "Prueba de alerta para la demo" \
  --region us-east-2
```

En menos de un minuto llega el correo `ALARM`. En la siguiente evaluación
(alrededor de 1 minuto, ya que no hay ejecuciones fallidas reales) la alarma
vuelve sola a `OK` y llega el correo `OK`. Para ver que la acción se ejecutó:

```bash
aws cloudwatch describe-alarm-history \
  --alarm-name payload-analytics-monitoring-pipeline-failed \
  --max-records 5 --region us-east-2 \
  --query 'AlarmHistoryItems[].[Timestamp,HistorySummary]' --output text
```

Debe aparecer `Successfully executed action arn:aws:sns:...:payload-analytics-alerts-dev`.

## Desplegar

Desde `infrastructure/serverless/monitoring/`, con las credenciales de AWS y
`SERVERLESS_ACCESS_KEY` exportadas y `npm install` hecho en `infrastructure/`:

```bash
DEVELOPER=nico11 DEPLOY_APP=payload-analytics AWS_DEFAULT_REGION=us-east-2 \
  npx serverless@4 deploy
```

El stack lee el ARN del tema SNS de SSM, así que el stack `alerts` debe existir
antes. Exporta a SSM `/payload-analytics-monitoring/DashboardName` y
`/payload-analytics-monitoring/PipelineFailedAlarmName`. Los umbrales están en
`custom.thresholds` de `serverless.yml`.

Para que el streaming publique métricas, reconstruye solo el servicio de Spark
desde `streaming/`:

```bash
docker compose -f .docker/docker-compose.yml up -d --build spark_streaming
```

El contenedor usa las credenciales de `.envs/aws.env`; el usuario necesita
`cloudwatch:PutMetricData`. Las métricas se publican igual si el job escribe en
una base local (`DB_HOST` en `streaming/.envs/config.env`); para que el
dashboard refleje el RDS, deja las variables `DB_*` sin definir.

## Evidencia

Imágenes generadas con `aws cloudwatch get-metric-widget-image` a partir de los
paneles del dashboard:

- `docs/monitoring/stepfunctions-executions.png`: ejecuciones del pipeline.
- `docs/monitoring/rds-cpu-storage.png`: CPU y espacio libre del RDS.
- `docs/monitoring/streaming-throughput.png`: filas escritas por minuto.
- `docs/monitoring/streaming-latency-lag.png`: latencia p95 y lag. El pico del
  primer minuto (p95 de 80 s) es el job poniéndose al día tras reiniciar el
  contenedor; después se estabiliza en 5 a 7 s de p95 y 1 a 5 s de lag, con
  unas 120 transacciones y 100 ticks FX por minuto.

Prueba de la alarma del pipeline (2026-11-08, hora de Colombia), salida de
`describe-alarm-history`:

```text
12:58:54  StateUpdate  Alarm updated from OK to ALARM
12:58:54  Action       Successfully executed action arn:aws:sns:us-east-2:682322555511:payload-analytics-alerts-dev
12:59:37  StateUpdate  Alarm updated from ALARM to OK
12:59:37  Action       Successfully executed action arn:aws:sns:us-east-2:682322555511:payload-analytics-alerts-dev
```
