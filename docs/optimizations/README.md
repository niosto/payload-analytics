# Optimizaciones de base de datos

Aplicamos tres optimizaciones con índices en PostgreSQL 18.3 (RDS `db.t4g.micro`). Cada una baja una consulta real del pipeline de segundos a milisegundos. Una cuarta (BRIN) quedó como experimento y no se aplicó.

| # | Consulta del pipeline | Antes | Después | Mejora | Dónde quedó aplicada |
| --- | --- | --- | --- | --- | --- |
| 1 | Última tasa FX por moneda (`fx_rates_current`) | 1.593 ms | **0,19 ms** | ~8.500× | Migración `20261023000000_stream_indexes` + reescritura con `LATERAL` en `fx_rates_current.sql` |
| 2 | Filtro incremental de `fct_live_transactions` | 712 ms | **0,37 ms** | ~1.900× | Migración `20261023000000_stream_indexes` + `post_hook` en `fct_live_transactions.sql` |
| 3 | Ingresos por país y mes (dashboard) | 1.827 ms | **49 ms** | ~37× | `post_hook` en `fct_transactions.sql` |
| 4 | Conteo por estado de las últimas 6 h | 14,6 ms | 12,7 ms | igual (índice 375× más pequeño) | No aplicada: experimento |

Los tiempos son la mediana de las corridas 2 a 4 de `EXPLAIN (ANALYZE, BUFFERS)`, en caliente. La corrida 1 (en frío) está en cada archivo de `resultados/`.

## Método

- **Datos.** El RDS tiene pocas filas (87.686 transacciones batch, ~7.000 en vivo); a ese tamaño casi cualquier plan tarda milisegundos. `sql/00_setup_perf.sql` crea el schema `perf` con el volumen que tendría el streaming tras días de operación: 2M ticks FX, 1M transacciones en vivo y `fct_transactions` ×20 (~1,8M filas, misma distribución). Cada tabla lleva los índices que ya existían, para que el "antes" sea el estado real.
- **Medición.** `run_explain.sh <nombre> <consulta.sql>` corre la consulta 4 veces con `EXPLAIN (ANALYZE, BUFFERS)` y `max_parallel_workers_per_gather = 0`, para que el paralelismo no tape el efecto del índice. La salida queda en `resultados/<nombre>.txt`.
- **Cada optimización** tiene su consulta (`sql/optN_consulta.sql`), su índice (`sql/optN_indice.sql`), el plan antes y después, el tiempo de creación y el tamaño del índice.

## 1. Última tasa FX por moneda

`fx_rates_current` toma, para cada moneda, el tick más reciente de las últimas 48 h. `to_usd` lo consulta en cada fila del batch.

- **Antes** (`opt1_antes.txt`): `DISTINCT ON (UPPER(TRIM(currency)))` sobre la ventana de 48 h. Lee las ~567.000 filas de la ventana y las ordena: **1.593 ms**.
- **Índice** (`opt1_indice.sql`): de expresión `(UPPER(TRIM(currency)), event_time DESC)`, con `INCLUDE (usd_per_unit, provider_updated_at)` y parcial `WHERE usd_per_unit > 0`. Coincide exactamente con la consulta.
- **Primer intento sin mejora** (`opt1_despues.txt`): 1.484 ms. El planner ni usó el índice nuevo. `DISTINCT ON` necesita todas las filas de la ventana, y un índice no sirve si de todos modos hay que leerlo entero.
- **Lo que funcionó** (`opt1_lateral_despues.txt`, consulta en `opt1_consulta_lateral.sql`): un `LATERAL ... ORDER BY ... LIMIT 1` por moneda. Así cada moneda hace una sola búsqueda en el índice; son 9 búsquedas y 36 páginas leídas: **0,19 ms**.
- **En el repo:** el índice va en la migración `20261023000000_stream_indexes` y la reescritura está en `backend/src/dbt/models/silver/fx_rates_current.sql`.
- **Lección:** el índice estaba bien hecho; la forma de la consulta impedía usarlo.

## 2. Filtro incremental de `fct_live_transactions`

En cada corrida, dbt busca las transacciones con `processed_at` posterior al máximo ya cargado menos 10 minutos.

- **Antes** (`opt2_antes.txt`): sin índice en `processed_at`, el `max()` y el filtro recorren el millón de filas (Seq Scan): **712 ms**, y **4.484 ms** en frío.
- **Índice** (`opt2_indice.sql`): B-tree en `processed_at` (21 MB).
- **Después** (`opt2_despues.txt`): el `max()` lee una sola entrada del índice (Index Only Scan Backward), y el filtro devuelve las 1.000 filas nuevas por índice: **0,37 ms**, y 11 ms en frío.
- **En el repo:** en `silver.stream_transactions`, la migración `20261023000000_stream_indexes`. En `gold.fct_live_transactions` hay un `post_hook` (`fct_live_transactions_processed_at_idx`), porque el `max()` corre sobre la tabla Gold.
- **Por qué `post_hook` y no `indexes`:** dbt solo crea los `indexes` de un modelo incremental cuando crea la tabla. La tabla del RDS ya existía, así que nunca habría recibido el índice.

## 3. Ingresos por país y mes

Es la consulta del dashboard: suma `revenue_amount` de las transacciones con ingreso, agrupada por país, año y mes.

- **Antes** (`opt3_antes.txt`): recorre todo `fct_transactions` (Seq Scan, ~1,8M filas) para quedarse con el ~4 % que tiene ingreso: **1.827 ms**.
- **Índice** (`opt3_indice.sql`): parcial `WHERE is_revenue`, en `(date_id)` con `INCLUDE (customer_country, revenue_amount)`. Solo indexa las filas con ingreso, así que pesa 2,2 MB frente a los 12-14 MB de los otros índices de la tabla, y responde sin tocar la tabla (Index Only Scan).
- **Después** (`opt3_despues.txt`): **49 ms**. Sobre los datos reales del RDS (`opt3_real_despues.txt`): **7,6 ms**.
- **En el repo:** un `post_hook` en `fct_transactions.sql`. La configuración `indexes` de dbt no admite índices parciales ni `INCLUDE`.
- **Error encontrado y corregido:** el `post_hook` corría dentro de la transacción de dbt. En ese momento la tabla de respaldo de la corrida anterior todavía existe y conserva el índice con el mismo nombre, así que `create index if not exists` no hacía nada; al borrar el respaldo, el índice desaparecía. Lo reprodujimos en local: el índice existía en las corridas 1 y 3 y faltaba en la 2. Ahora el hook corre con `transaction: False`, después de que se borra el respaldo, y el índice queda en todas las corridas.

## 4. BRIN en `event_time` (experimento, no aplicado)

- **Consulta:** conteo por estado de las transacciones de las últimas 6 h.
- **Antes** (`opt4_antes.txt`): B-tree en `event_time`, de 21 MB: **14,6 ms**.
- **Después** (`opt4_despues.txt`): BRIN con `pages_per_range = 32`, de **56 kB**: **12,7 ms**.
- **Resultado:** el mismo tiempo con un índice 375 veces más pequeño. Funciona porque el stream solo agrega filas en orden de tiempo (correlación 1, en `opt4_correlacion.txt`).
- **Por qué no se aplicó:** con el volumen actual el B-tree no estorba. Es la opción a tomar si `stream_transactions` crece al orden de los millones de filas.

## Reproducir

```bash
psql -f sql/00_setup_perf.sql                      # crea perf.* (~930 MB)
./run_explain.sh opt2_antes sql/opt2_consulta.sql
psql -f sql/opt2_indice.sql
./run_explain.sh opt2_despues sql/opt2_consulta.sql
```

`run_explain.sh` usa las variables `PG*` de `psql` para conectarse. Al terminar, borra el schema de prueba: `DROP SCHEMA perf CASCADE;`. En el RDS compartido ocupa unos 928 MB de 1 GB.
