# Guion de la demo en vivo (15 minutos)

La demo muestra el flujo completo de punta a punta: **captura** (tasas reales y transacciones en vivo, más un archivo batch), **limpieza**, **almacenamiento** en Silver y Gold, **consumo** en Power BI, **CI/CD** (un cambio fusionado dispara las pruebas y el despliegue solos) y **monitoreo y alertas**.

Reglas del formato: **15 minutos como máximo, sin diapositivas, participan los cuatro integrantes, y el alistamiento cuenta como parte del tiempo.** Todo se hace con el sistema real, sin presentaciones.

## Quién hace qué

La asignación sigue la matriz RACI ([`governance/raci.md`](governance/raci.md)).

| Integrante | Bloque |
| --- | --- |
| Nicolás Saldarriaga | Batch: subir el archivo, dato crudo contra limpio, Gold y una optimización |
| Samuel Deossa | Streaming: Redpanda Console, latencia, transacciones y tasas FX en vivo |
| Nicolás Ospina | CI/CD (fusionar en vivo y mostrar el despliegue) y monitoreo (dashboard y alarma) |
| Samuel Henao | Consumo: Power BI, catálogo y linaje |

**Pantalla.** Hay que decidir en el primer ensayo qué PC proyecta cada bloque y cómo se cambia de uno a otro. Lo recomendable es que el streaming, AWS y GitHub estén en un mismo PC (el que ya tiene los archivos `.envs` y Docker) y que Power BI tenga `dashboard.pbix` abierto en ese mismo PC, para no perder tiempo cambiando de pantalla.

## Cronograma

| Min | Qué | Quién |
| --- | --- | --- |
| 0–1 | Todo precalentado: pestañas abiertas, streaming encendido, Power BI abierto | Todos |
| 1–5 | Subir el JSON a `raw/`; con Step Functions en curso, mostrar el streaming (Redpanda Console, latencia, transacciones y tasas FX en vivo) | Saldarriaga y Deossa |
| 5–8 | Resultado del batch: dato crudo contra limpio, Gold y una optimización (`EXPLAIN`) | Saldarriaga |
| 8–12 | **CI/CD:** la PR ya está abierta con un cambio inocuo y `test` en verde. Fusionar en vivo, mostrar el job `deploy`, el resumen (imagen = SHA del commit) y la ejecución verde previa como respaldo | Ospina |
| 12–13 | Monitoreo: dashboard de CloudWatch y `set-alarm-state`, con el correo de alerta | Ospina |
| 13–15 | Consumo: Power BI refrescado contra la RDS, catálogo y linaje | Henao |

## Reglas que no se rompen

1. **No subir el JSON mientras el `deploy` está en curso.** El despliegue cambia la task de dbt y la state machine, y una ejecución que arranca en ese momento puede fallar. Por eso el batch va **antes** de fusionar (minutos 1–5) y el merge después.
2. **El batch tiene que haber terminado antes del minuto 8.** Se sube en el minuto 1, así que tiene unos 7 minutos. Si el ensayo mide más de 6, el archivo se sube durante el alistamiento (minuto 0). Si a la hora del bloque sigue *Running*, no se espera: se muestra la ejecución anterior que ya está en *Succeeded* y se sigue.
3. **Cualquier cambio fusionado a `main` despliega.** El workflow no filtra por rutas, así que incluso un cambio de documentación reconstruye la imagen de dbt y despliega la state machine. Por eso el cambio de la demo puede ser inocuo (por ejemplo, una línea de este documento): lo que se muestra es la tubería, no el cambio.
4. **`main` queda congelado** desde el último despliegue verde y el pipeline de prueba. El único cambio que entra después es el de la demo.
5. **Si `test` o `deploy` superan el tiempo asignado**, no se espera con la pantalla quieta. Se deja la ejecución en curso a la vista, se explica qué paso está corriendo y se muestra la ejecución anterior en verde (ver la tabla siguiente).

### Duraciones a medir en los ensayos

Los llena el equipo. No se inventan: se anotan de las ejecuciones reales de Actions.

| Qué | Duración | Cómo se mide |
| --- | --- | --- |
| Job `test` (pytest y dbt sobre la fixture) | TODO medir | Actions → la ejecución → duración del job `test` |
| Job `deploy` (migración, imagen, dbt, state machine) | TODO medir | Actions → la ejecución → duración del job `deploy` |
| Batch completo (Glue y dbt) | TODO medir. Referencias que no coinciden: unos 4 min (guion anterior) y 7 min (modelo de costo: Glue 4 más dbt 2,5) | Step Functions → duración de la ejecución |
| Latencia p95 de transacciones en el streaming | TODO medir | Consulta del bloque 1–5. La referencia es menos de 5 s |

**Qué hacer si superan el tiempo.** Si `test` o `deploy` ya pasaron de su duración medida más un margen, se pasa al respaldo: la ejecución verde previa de la misma PR o de `main`, con su resumen y su imagen. Se dice en voz alta que el despliegue sigue corriendo y se continúa con el monitoreo. El plan es que el CI/CD entre en 4 minutos aunque no termine.

## Alistamiento

### El día anterior

- [ ] Una ejecución verde de `test` y de `deploy` en `main`, con capturas y duraciones anotadas.
- [ ] `main` protegido (PR obligatorio y check `test`) y congelado.
- [ ] La PR de la demo abierta con un cambio inocuo y `test` en verde.
- [ ] El pipeline probado con el código final: JSON a `raw/`, archivo en `processed/`, y `gold.dim_fx_rate_current` con las tasas.
- [ ] `dashboard.pbix` apuntando a la RDS, refrescado y guardado (ver [README](../README.md#dashboard)).
- [ ] Dos ensayos cronometrados con los cuatro integrantes.
- [ ] Un ensayo completo grabado y capturas de cada bloque (plan B).

### 30 minutos antes

1. Abrir **Docker Desktop** y esperar a que diga *Engine running*.
2. Prender el streaming desde la carpeta `streaming`. Tiene que estar corriendo al menos 10 minutos antes de empezar, para que haya tasas recientes:
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
5. Confirmar que hay salida al puerto 5432 de la RDS desde la red del lugar. Si la red lo bloquea, usar el punto de acceso del teléfono.

### 15 minutos antes: dejar abiertas estas pestañas

| Pestaña | Dirección o cómo abrirla |
| --- | --- |
| Repositorio y la PR de la demo | https://github.com/niosto/payload-analytics (pestaña *Pull requests*) |
| GitHub Actions | https://github.com/niosto/payload-analytics/actions, con la última ejecución verde de `main` a la vista |
| Redpanda Console | http://localhost:8088 → *Topics* |
| Step Functions | Consola AWS, región **us-east-2 (Ohio)** → Step Functions → `payload-analytics-backend-orchestration` → *Executions* |
| S3 | Consola AWS → S3 → el bucket del data lake (`raw/`, `processed/`) |
| CloudWatch | Dashboard `payload-analytics`, región `us-east-2` (enlace en [`monitoring.md`](monitoring.md#cómo-abrir-el-dashboard)) |
| Correo | La bandeja del `ALERTS_EMAIL`, para ver llegar `ALARM` y luego `OK` |
| pgAdmin 4 | Conectado a la RDS, con las consultas de este guion ya pegadas en pestañas |
| Power BI | `powerbi/dashboard.pbix` abierto y refrescado |
| Catálogo y linaje | `docs/governance/catalog.html` en el navegador y [`governance/lineage.md`](governance/lineage.md) |
| PowerShell | En la carpeta del repo, listo para el comando del bloque 1–5 |

**Conexión de pgAdmin a la RDS.** Host, base `payload_analytics`, usuario `payload_analytics_admin`, puerto 5432, SSL `require`. El host y la contraseña se sacan **antes**, nunca en pantalla:

```powershell
docker exec PayloadAnalyticsBackend aws ssm get-parameter --name /payload-analytics-rds/DBInstanceEndpoint --query Parameter.Value --output text
docker exec PayloadAnalyticsBackend aws secretsmanager get-secret-value --secret-id /payload-analytics-rds/MasterCredentials --query SecretString --output text
```

Se usa pgAdmin o el `psql` instalado en el PC. El contenedor de backend no trae `psql`.

### Chequeo final, 5 minutos antes

En pgAdmin, las 9 monedas deben decir `stream`. Si alguna dice `seed`, el streaming no lleva suficiente tiempo corriendo; esperar y repetir.

```sql
select currency, rate_source from silver.fx_rates_current order by currency;
```

Y que la alarma de prueba esté en `OK` antes de forzarla:

```powershell
docker exec PayloadAnalyticsBackend aws cloudwatch describe-alarms --alarm-names payload-analytics-monitoring-pipeline-failed --query "MetricAlarms[0].StateValue" --output text
```

## Minutos 1–5: batch y streaming (Saldarriaga y Deossa)

**Saldarriaga lanza el batch.** En PowerShell, desde la carpeta del repo:

```powershell
docker exec PayloadAnalyticsBackend bash -lc 'B=$(aws ssm get-parameter --name /payload-analytics-storage/DataLakeBucketName --query Parameter.Value --output text); aws s3 cp /app/data/raw/fintech_banking_dataset.json s3://$B/raw/'
```

Copiarlo tal cual. Windows PowerShell 5.1 borra las comillas dobles dentro de los argumentos de `docker`, así que el comando no lleva ninguna.

Pasar a **Step Functions** y refrescar: aparece una ejecución nueva en *Running*. Abrirla para mostrar el grafo: Glue → dbt → mover el archivo.

> "No se ejecuta nada a mano: EventBridge ve el archivo en `raw/` y arranca Step Functions."

**Deossa muestra el streaming mientras el batch corre.** En **Redpanda Console**, abrir el tópico `fx.rates` y luego `fintech.live_transactions`. Mostrar que los mensajes siguen llegando y que el campo `provider` dice `binance`. Luego, en pgAdmin, los últimos ticks guardados en la RDS:

```sql
select currency, provider, round((1/usd_per_unit)::numeric, 2) as por_usd,
       provider_updated_at, event_time
from silver.stream_fx_rates
order by event_time desc
limit 9;
```

> "BRL, ARS, MXN, COP y EUR vienen de trades reales de Binance; USD, CLP, PEN y UYU de una API diaria. `provider_updated_at` es la hora real del precio."

Los logs de Spark y la latencia de los últimos 10 minutos:

```powershell
docker logs --tail 5 PayloadAnalyticsSparkStreaming
```

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

> "Spark Structured Streaming une cada transacción con la tasa de su moneda del minuto anterior. Es un left join: si la fuente de tasas se cae, la transacción no se pierde. El objetivo era latencia de FX menor a 5 segundos."

## Minutos 5–8: resultado del batch (Saldarriaga)

Antes de empezar, confirmar en **Step Functions** que la ejecución está en *Succeeded* y en **S3** que el archivo pasó de `raw/` a `processed/`. Si sigue *Running*, aplicar la regla 2.

**Dato crudo contra limpio**, las mismas filas en `stg_transactions` y en `transactions`:

```sql
select s.transaction_id, s.date as fecha_cruda, s.amount as monto_crudo, s.type as tipo_crudo,
       t.transaction_date, t.amount, round(t.amount_usd::numeric, 2) as amount_usd, t.type
from silver.stg_transactions s
join silver.transactions t on t.transaction_id = trim(s.transaction_id)
where s.amount like '$%' and s.date like '%/%' and lower(s.type) <> lower(t.type)
limit 3;
```

> "Entra `'$309336.16'`, fecha `22/11/2025` y tipo `Deposito`; sale un número, una fecha y `deposit`, más el monto en USD. Spark solo aplana; toda la limpieza es dbt, con macros y seeds versionados."

**Gold**, la tasa vigente y el hecho en vivo:

```sql
select currency, rate_source, round((1/usd_per_unit)::numeric, 2) as por_usd, rate_event_time
from gold.dim_fx_rate_current
order by currency;

select transaction_id, currency, amount_native, round(amount::numeric, 2) as amount_usd,
       fx_rate_source, round(latency_seconds::numeric, 1) as latencia_s, event_time
from gold.fct_live_transactions
order by event_time desc
limit 5;
```

**Una optimización funcionando**, con el índice parcial en el plan:

```sql
explain analyze
select f.customer_country, d.year, d.month_number, sum(f.revenue_amount)
from gold.fct_transactions f join gold.dim_date d using (date_id)
where f.is_revenue
group by 1, 2, 3;
```

> "Todo Gold queda en USD con la tasa en vivo. La consulta de ingresos usa el índice parcial `fct_transactions_revenue_idx`. Las mediciones antes y después están en `docs/optimizations/`."

## Minutos 8–12: CI/CD (Ospina)

Precondición: el batch terminó (regla 2) y la PR de la demo tiene `test` en verde.

1. Mostrar la PR abierta: el check `test` en verde y el cambio inocuo. Decir que la PR no recibe credenciales de AWS.
2. **Fusionar la PR en vivo.** Pasar a **Actions**: arranca la ejecución sobre `main` con `test` y, cuando termina, `deploy`.
3. Mientras corre, explicar cada paso del job `deploy`: asume el rol por OIDC (sin llaves guardadas), aplica la migración, sube la imagen de dbt con el SHA del commit como etiqueta y despliega la task y la state machine. Los pasos están en [`cicd.md`](cicd.md).
4. Al terminar, abrir el **resumen del job**: la imagen de dbt termina en el SHA del commit que se acaba de fusionar, y la task definition es la nueva revisión.
5. Respaldo (regla 5): la ejecución verde previa de `main`, con su resumen, ya abierta en otra pestaña.

> "Un cambio en `main` prueba y despliega solo. El rol de despliegue solo lo puede asumir la rama `main` de este repositorio, y la protección de la rama exige PR y el check `test`."

No subir ningún archivo a `raw/` hasta que `deploy` esté en verde (regla 1).

## Minutos 12–13: monitoreo y alertas (Ospina)

1. Abrir el dashboard de CloudWatch `payload-analytics`: ejecuciones del pipeline, RDS y las métricas del streaming (filas por minuto, latencia p95 y lag).
2. Forzar la alarma del pipeline, con el correo abierto en otra pestaña:

```powershell
docker exec PayloadAnalyticsBackend aws cloudwatch set-alarm-state --alarm-name payload-analytics-monitoring-pipeline-failed --state-value ALARM --state-reason "Prueba de alerta para la demo" --region us-east-2
```

En menos de un minuto llega el correo `ALARM`; al evaluarse de nuevo, la alarma vuelve sola a `OK` y llega el segundo correo. Detalle y comando de verificación en [`monitoring.md`](monitoring.md#probar-una-alerta-demo).

> "Cuatro alarmas: pipeline fallido, poco disco en la RDS, CPU alta y lag del streaming. Todas llegan por correo al mismo tema SNS."

Si el correo tarda, mostrar el dashboard con la alarma en rojo y el historial (`describe-alarm-history`), que registra la acción SNS ejecutada.

## Minutos 13–15: consumo (Henao)

1. **Power BI** (`dashboard.pbix`) ya refrescado contra la RDS: recorrer las cuatro páginas en 1 minuto. Dejar más tiempo en *Operación en tiempo real*, que muestra lo que acabamos de ver en vivo, y en *Calidad y gobierno*, con las 212 reglas de dbt.
2. **Catálogo y linaje** en 1 minuto: abrir `docs/governance/catalog.html`, buscar `dim_fx_rate_current` y mostrar su linaje hasta Silver.

> "El dashboard lee Gold, la misma capa que acabamos de ver. El responsable de datos ve ahí si puede confiar en las cifras antes de compartirlas."

## Si algo sale mal (plan B)

| Problema | Qué hacer |
| --- | --- |
| El pipeline no termina a tiempo | Mostrar la ejecución anterior que ya está en *Succeeded* y seguir; no esperar. |
| `test` o `deploy` demoran más de lo medido | Regla 5: dejar la ejecución a la vista, mostrar la ejecución verde previa y su resumen, y seguir con el monitoreo. |
| Spark no aparece en `docker ps` | `docker compose -f .docker/docker-compose.yml up -d spark_streaming` desde `streaming`. Retoma desde su checkpoint sin duplicar. |
| No hay ticks nuevos de Binance | La API diaria sigue publicando, y `fx_rates_current` usa la última tasa de las últimas 48 h. Explicarlo como la tolerancia a fallos del diseño. |
| No hay internet | Mostrar la **grabación del ensayo completo** y las capturas de cada bloque. |
| pgAdmin no conecta | Usar `psql.exe` del PC con los mismos datos, o el punto de acceso del teléfono si la red bloquea el puerto 5432. |
| Power BI no actualiza contra la RDS | Abrir el `.pbix` tal cual: guarda una copia de los datos y muestra las cuatro páginas sin conexión. |
| El correo de alarma no llega | Mostrar la alarma en `ALARM` en CloudWatch y el historial con la acción SNS. |

## Preguntas probables

- **¿Por qué el streaming corre local y no en AWS?** MSK cuesta entre USD 70 y 500 al mes y la alerta de presupuesto está en USD 1. Local cuesta casi nada en AWS y escribe en la misma RDS, así que dbt no nota la diferencia.
- **¿Qué pasa si se cae la fuente de tasas?** El left join escribe la transacción igual, sin tasa, cuando pasa el watermark de 2 minutos, y dbt la convierte con `fx_rates_current`. Se probó con 200 s de caída: no se perdió ninguna.
- **¿Cómo evitan duplicados si Spark se reinicia?** Checkpoint de offsets más upsert por `transaction_id`. Se probó reiniciando el job: 0 duplicados.
- **¿Por qué el índice de la tasa FX no servía al principio?** `DISTINCT ON` lee toda la ventana de 48 h. Reescribirlo con `LATERAL ... LIMIT 1` por moneda hace que use el índice: de 1.593 ms a 0,19 ms.
- **¿Las transacciones son reales?** No. Son simuladas, pero sobre clientes y cuentas reales del dataset y con su misma suciedad. Las tasas sí son reales.
- **¿Qué impide que cualquiera despliegue a AWS?** No hay llaves de AWS en GitHub. El rol solo se asume por OIDC desde la rama `main` de este repositorio, y la protección de `main` exige PR y el check `test`. Ese control es lo que sostiene la seguridad del rol; está explicado en [`cicd.md`](cicd.md).
- **¿Qué pasa si el despliegue falla?** CloudFormation revierte el stack fallido a la versión anterior, así que el pipeline sigue usando la última task definition buena. El paso que falló aparece con su nombre en el job.
- **¿Qué pruebas corren?** 33 de `pytest` (productores y transformaciones de Spark) y una integración con 212 pruebas de dbt sobre una base temporal. Ninguna se salta ni se relaja.
- **¿Qué no detecta el monitoreo?** La alarma de lag no avisa si el job de streaming se cae por completo (no publica métricas); eso se ve en el dashboard, en el panel de filas escritas vacío.
- **¿Cuánto cuesta operarlo?** USD 18,54 al mes modelados frente a USD 18,01 facturados (Cost Explorer). RDS es la mayor parte; el modelo para 1×, 10× y 100× está en [`cost/`](cost/README.md).

## Después de la demo

Apagar el streaming para no llenar el disco de la RDS (unos 100 MB por día si queda prendido), desde `streaming`:

```powershell
docker compose -f .docker/docker-compose.yml stop
```
