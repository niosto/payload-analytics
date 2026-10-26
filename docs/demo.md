# Guion de la demo en vivo

La demo muestra el flujo de punta a punta en unos **6 minutos**, dentro de los 20 de la exposición: **captura** (tasas reales y transacciones en vivo, más un archivo batch), **limpieza** (dato sucio contra dato limpio) y **almacenamiento** (Silver y Gold en RDS).

El profesor elige en el momento quién la hace. Por eso se hace **siempre en el mismo PC**, el que ya tiene los archivos `.envs` y Docker configurados (hoy, el de Samuel Deossa), y quien salga elegido sigue este guion tal cual.

## Antes de la clase

### 30 minutos antes

1. Abrir **Docker Desktop** y esperar a que diga *Engine running*.
2. Prender el streaming desde la carpeta `streaming`. Tiene que estar corriendo al menos 10 minutos antes, para que haya tasas recientes:
   ```powershell
   docker compose -f .docker/docker-compose.yml up -d
   ```
3. Prender el contenedor de backend desde la carpeta `backend`:
   ```powershell
   docker compose -f .docker/docker-compose.yml up -d
   ```
4. Ver que Spark esté escribiendo. Deben salir líneas `tx batch=... rows=...`:
   ```powershell
   docker logs --tail 5 PayloadAnalyticsSparkStreaming
   ```

### 15 minutos antes: dejar abiertas estas pestañas

| Pestaña | Dirección o cómo abrirla |
| --- | --- |
| Repositorio | https://github.com/niosto/payload-analytics |
| Redpanda Console | http://localhost:8088 → *Topics* |
| Step Functions | Consola AWS, región **us-east-2 (Ohio)** → Step Functions → `payload-analytics-backend-orchestration` → *Executions* |
| S3 | Consola AWS → S3 → `payload-analytics-datalake-dev-nico11` |
| pgAdmin 4 | Conectado al RDS (ver abajo), con las consultas de este guion ya pegadas en pestañas |
| PowerShell | En la carpeta del repo, listo para el comando del paso 2 |

**Conexión de pgAdmin al RDS.** Host, base `payload_analytics`, usuario `payload_analytics_admin`, puerto 5432, SSL `require`. El host y la contraseña se sacan **antes** de la clase, nunca en pantalla:

```powershell
docker exec PayloadAnalyticsBackend aws ssm get-parameter --name /payload-analytics-rds/DBInstanceEndpoint --query Parameter.Value --output text
docker exec PayloadAnalyticsBackend aws secretsmanager get-secret-value --secret-id /payload-analytics-rds/MasterCredentials --query SecretString --output text
```

Se usa pgAdmin o el `psql` del PC (`C:\Program Files\PostgreSQL\18\bin\psql.exe`). El contenedor de backend no trae `psql`.

### Chequeo final, 5 minutos antes

En pgAdmin, las 9 monedas deben decir `stream`. Si alguna dice `seed`, el streaming no lleva suficiente tiempo corriendo; esperar y repetir.

```sql
select currency, rate_source from silver.fx_rates_current order by currency;
```

## La demo, paso a paso

| # | Tiempo | Qué hacer | Qué decir (en una frase) |
| --- | --- | --- | --- |
| 1 | 1 min | Captura en tiempo real | Las tasas son reales y llegan cada segundo |
| 2 | 30 s | Lanzar el batch | Subir un archivo a S3 dispara todo el pipeline en AWS |
| 3 | 2 min | Streaming mientras corre el batch | Cada transacción se cruza con la tasa del minuto anterior |
| 4 | 1 min | Limpieza | El dato sucio entra como texto y sale tipado |
| 5 | 1 min | Almacenamiento en Gold | Gold queda en USD con la tasa en vivo y responde en milisegundos |
| 6 | 30 s | Cierre | El pipeline terminó y pasaron las 206 reglas de calidad |

### 1. Captura en tiempo real (1 min)

En **Redpanda Console**, abrir el tópico `fx.rates` y luego `fintech.live_transactions`. Mostrar que los mensajes siguen llegando y que el campo `provider` dice `binance`.

Luego, en pgAdmin, los últimos ticks guardados en el RDS:

```sql
select currency, provider, round((1/usd_per_unit)::numeric, 2) as por_usd,
       provider_updated_at, event_time
from silver.stream_fx_rates
order by event_time desc
limit 9;
```

> "BRL, ARS, MXN, COP y EUR vienen de trades reales de Binance; USD, CLP, PEN y UYU de una API diaria. `provider_updated_at` es la hora real del precio."

### 2. Lanzar el batch (30 s)

En PowerShell, desde la carpeta del repo:

```powershell
docker exec PayloadAnalyticsBackend bash -lc 'B=$(aws ssm get-parameter --name /payload-analytics-storage/DataLakeBucketName --query Parameter.Value --output text); aws s3 cp /app/data/raw/fintech_banking_dataset.json s3://$B/raw/'
```

Copiarlo tal cual. Windows PowerShell 5.1 borra las comillas dobles dentro de los argumentos de `docker`, así que el comando no lleva ninguna.

Pasar a **Step Functions** y refrescar: aparece una ejecución nueva en *Running*. Abrirla para mostrar el grafo: Glue → dbt → mover el archivo.

> "No se ejecuta nada a mano: EventBridge ve el archivo en `raw/` y arranca Step Functions. Glue tarda unos 2 minutos y dbt otros 2."

### 3. Streaming mientras corre el batch (2 min)

Mostrar los logs de Spark:

```powershell
docker logs --tail 5 PayloadAnalyticsSparkStreaming
```

Y la latencia de los últimos 10 minutos:

```sql
select 'transacciones' as flujo, count(*),
       round(percentile_cont(0.95) within group (order by extract(epoch from processed_at - event_time))::numeric, 2) as p95_seg
from silver.stream_transactions
where event_time > now() - interval '10 minutes' and fx_usd_per_unit is not null
union all
select 'tasas FX', count(*),
       round(percentile_cont(0.95) within group (order by extract(epoch from processed_at - event_time))::numeric, 2)
from silver.stream_fx_rates
where event_time > now() - interval '10 minutes';
```

> "Spark Structured Streaming une cada transacción con la tasa de su moneda del minuto anterior. Es un left join: si la fuente de tasas se cae, la transacción no se pierde. El KPI era latencia de FX menor a 5 segundos y estamos alrededor de 2 a 4."

### 4. Limpieza (1 min)

Lo crudo (`stg_transactions`) contra lo limpio (`transactions`) para las mismas filas:

```sql
select s.transaction_id, s.date as fecha_cruda, s.amount as monto_crudo, s.type as tipo_crudo,
       t.transaction_date, t.amount, round(t.amount_usd::numeric, 2) as amount_usd, t.type
from silver.stg_transactions s
join silver.transactions t on t.transaction_id = trim(s.transaction_id)
where s.amount like '$%' and s.date like '%/%' and lower(s.type) <> lower(t.type)
limit 3;
```

> "Entra `'$309336.16'`, fecha `22/11/2025` y tipo `Deposito`; sale un número, una fecha y `deposit`, más el monto en USD. Spark solo aplana; toda la limpieza es dbt, con macros y seeds versionados."

### 5. Almacenamiento en Gold (1 min)

La tasa vigente y el hecho en vivo:

```sql
select currency, rate_source, round((1/usd_per_unit)::numeric, 2) as por_usd, rate_event_time
from silver.fx_rates_current
order by currency;

select transaction_id, currency, amount_native, round(amount::numeric, 2) as amount_usd,
       fx_rate_source, round(latency_seconds::numeric, 1) as latencia_s, event_time
from gold.fct_live_transactions
order by event_time desc
limit 5;
```

Y una optimización funcionando, con el índice parcial en el plan:

```sql
explain analyze
select f.customer_country, d.year, d.month_number, sum(f.revenue_amount)
from gold.fct_transactions f join gold.dim_date d using (date_id)
where f.is_revenue
group by 1, 2, 3;
```

> "Todo Gold queda en USD con la tasa en vivo. La consulta de ingresos usa el índice parcial `fct_transactions_revenue_idx`: unos 10 ms sobre los datos reales."

### 6. Cierre (30 s)

Volver a **Step Functions**: la ejecución debe estar en **Succeeded**. En **S3**, mostrar que el archivo pasó de `raw/` a `processed/`.

> "dbt corrió las 206 reglas de calidad dentro del pipeline. Si una fallaba, el archivo iba a `failed/` y llegaba un correo por SNS."

## Si algo sale mal

| Problema | Qué hacer |
| --- | --- |
| El pipeline no termina a tiempo | Mostrar la ejecución anterior que ya está en *Succeeded* y seguir; no esperar. |
| Spark no aparece en `docker ps` | `docker compose -f .docker/docker-compose.yml up -d spark_streaming` desde `streaming`. Retoma desde su checkpoint sin duplicar. |
| No hay ticks nuevos de Binance (red de la universidad) | La API diaria sigue publicando, y `fx_rates_current` usa la última tasa de las últimas 48 h. Explicarlo como la tolerancia a fallos del diseño. |
| No hay internet | Mostrar capturas tomadas antes: Redpanda Console, Step Functions en *Succeeded*, y los resultados de los pasos 4 y 5. |
| pgAdmin no conecta | Usar `psql.exe` del PC con los mismos datos. |

## Preguntas probables

- **¿Por qué el streaming corre local y no en AWS?** MSK cuesta entre USD 70 y 500 al mes y la alerta de presupuesto está en USD 1. Local cuesta casi nada en AWS y escribe en el mismo RDS, así que dbt no nota la diferencia.
- **¿Qué pasa si se cae la fuente de tasas?** El left join escribe la transacción igual, sin tasa, cuando pasa el watermark de 2 minutos, y dbt la convierte con `fx_rates_current`. Se probó con 200 s de caída: no se perdió ninguna.
- **¿Cómo evitan duplicados si Spark se reinicia?** Checkpoint de offsets más upsert por `transaction_id`. Se probó reiniciando el job: 0 duplicados.
- **¿Por qué el índice de la tasa FX no servía al principio?** `DISTINCT ON` lee toda la ventana de 48 h. Reescribirlo con `LATERAL ... LIMIT 1` por moneda hace que use el índice: de 1.593 ms a 0,19 ms.
- **¿Las transacciones son reales?** No. Son simuladas, pero sobre clientes y cuentas reales del dataset y con su misma suciedad. Las tasas sí son reales.

## Después de la clase

Apagar el streaming para no llenar el disco del RDS (unos 100 MB por día si queda prendido), desde `streaming`:

```powershell
docker compose -f .docker/docker-compose.yml stop
```
