### Tabla 1. Costo mensual por componente segun el volumen de ingesta (consumo base: 8 actualizaciones/dia, 3 analistas)

| Componente (USD/mes) | 1x (0.05 GB/dia; 2 ev/s) | 10x (0.5 GB/dia; 20 ev/s) | 100x (5 GB/dia; 200 ev/s) | 1000x (50 GB/dia; 2000 ev/s) |
|---|---|---|---|---|
| RDS instancia | 11.52 | 11.52 | 92.88 | 688.32 |
| RDS almacenamiento | 2.30 | 2.30 | 8.28 | 81.53 |
| Glue batch | 1.76 | 3.74 | 27.39 | 271.59 |
| Fargate dbt | 0.03 | 0.09 | 0.64 | 6.20 |
| S3 | 0.26 | 2.09 | 20.38 | 203.27 |
| Transferencia saliente | 0.00 | 0.00 | 0.00 | 0.00 |
| IPv4 publica | 3.61 | 3.62 | 3.73 | 4.86 |
| Otros | 0.84 | 0.84 | 0.85 | 2.47 |
| **Total (streaming local)** | **20.32** | **24.20** | **154.16** | **1,258.24** |

### Tabla 2. Puntos de quiebre de la arquitectura barata

| Ingesta | Instancia RDS requerida | Base (GB) | Disco (GB) | Streaming (GB/dia) | Dias para llenar 20 GB sin purga | Workers Glue | Duracion corrida (min) | Excede SLA 60 min | Exige partir el JSON |
|---|---|---|---|---|---|---|---|---|---|
| 1x | db.t4g.micro | 1.3 | 20 | 0.09 | 163 | 2 | 7 | No | No |
| 10x | db.t4g.micro | 6.4 | 20 | 0.63 | 22 | 2 | 16 | No | No |
| 100x | db.t4g.large | 57.3 | 72 | 5.97 | ya excede | 7 | 70 | Si | Si |
| 1000x | db.r7g.2xlarge | 566.4 | 709 | 59.42 | ya excede | 67 | 521 | Si | Si |

### Tabla 3. Costo mensual total (USD) segun donde corre el streaming

| Arquitectura de streaming | 1x (2 ev/s) | 10x (20 ev/s) | 100x (200 ev/s) | 1000x (2000 ev/s) |
|---|---|---|---|---|
| Local (Docker), 0 en AWS | 20.32 | 24.20 | 154.16 | 1,258.24 |
| MSK aprovisionado + Glue Streaming | 754.08 | 758.77 | 1,239.08 | 3,375.57 |
| MSK Serverless + Glue Streaming | 1,200.59 | 1,208.84 | 1,382.54 | 3,874.42 |
| Kinesis on-demand + Glue Streaming | 684.86 | 692.02 | 854.78 | 3,237.31 |
| Kinesis aprovisionado + Glue Streaming | 666.35 | 670.88 | 807.37 | 2,948.77 |

### Tabla 3b. Sobrecosto mensual (USD) de llevar el streaming a AWS (vs. local)

| Arquitectura de streaming | 1x | 10x | 100x | 1000x |
|---|---|---|---|---|
| MSK aprovisionado + Glue Streaming | 733.76 | 734.58 | 1,084.92 | 2,117.33 |
| MSK Serverless + Glue Streaming | 1,180.27 | 1,184.64 | 1,228.38 | 2,616.18 |
| Kinesis on-demand + Glue Streaming | 664.54 | 667.82 | 700.62 | 1,979.07 |
| Kinesis aprovisionado + Glue Streaming | 646.03 | 646.68 | 653.21 | 1,690.53 |

### Tabla 4. Costo mensual segun el consumo (ingesta hoy 1x y 100x, streaming local)

| Escenario | Actualiz./dia | Analistas | 1x: GB salida/mes | 1x: RDS | 1x: salida USD | 1x: total USD | 100x: GB salida/mes | 100x: RDS | 100x: salida USD | 100x: total USD |
|---|---|---|---|---|---|---|---|---|---|---|
| C1 minimo | 1 | 1 | 2 | db.t4g.micro | 0.00 | 20.32 | 2 | db.t4g.large | 0.00 | 154.16 |
| C2 base | 8 | 3 | 14 | db.t4g.micro | 0.00 | 20.32 | 14 | db.t4g.large | 0.00 | 154.16 |
| C3 medio | 48 | 10 | 77 | db.t4g.small | 0.00 | 31.84 | 77 | db.r7g.large | 0.00 | 233.36 |
| C4 alto | 96 | 25 | 156 | db.r7g.large | 5.08 | 185.97 | 156 | db.r7g.large | 5.08 | 238.44 |
| C5 muy alto | 288 | 50 | 457 | db.r7g.large | 32.13 | 213.01 | 457 | db.r7g.large | 32.13 | 265.49 |

### Tabla 5. Analistas concurrentes que obligan a cambiar de instancia (ingesta 1x, 8 actualiz./dia)

| Desde N analistas | Instancia RDS | Carga CPU promedio (vCPU) | RDS instancia USD/mes |
|---|---|---|---|
| 1 | db.t4g.micro | 0.03 | 11.52 |
| 7 | db.t4g.small | 0.15 | 23.04 |
| 14 | db.t4g.large | 0.29 | 92.88 |
| 21 | db.r7g.large | 0.43 | 172.08 |
| 70 | db.r7g.xlarge | 1.41 | 344.16 |
| 140 | db.r7g.2xlarge | 2.81 | 688.32 |

### Tabla 6. Crecimiento de RDS por streaming (sin purga)

| Eventos/s | Filas/s | MB/dia | GB/mes | Dias hasta llenar 20 GB |
|---|---|---|---|---|
| 2 | 3.5 | 96 | 2.8 | 163 |
| 20 | 21.5 | 644 | 18.9 | 22 |
| 200 | 201.5 | 6,117 | 179.2 | ya excede (el batch ocupa el disco) |
| 2000 | 2,001.5 | 60,845 | 1,782.6 | ya excede (el batch ocupa el disco) |

### Tabla 7. Duracion de una corrida batch segun el volumen

| Ingesta | GB por corrida | Workers Glue | Glue (min) | dbt (min) | Total (min) |
|---|---|---|---|---|---|
| 1x | 0.05 | 2 | 4.0 | 2.5 | 7 |
| 10x | 0.5 | 2 | 8.5 | 7.0 | 16 |
| 100x | 5 | 7 | 17.8 | 52.0 | 70 |
| 1000x | 50 | 67 | 18.4 | 502.0 | 521 |

### Tabla 8. Elasticidad: cuanto crece el costo cuando un factor se multiplica por 10

| Cambio | Costo antes (USD/mes) | Costo despues (USD/mes) | Factor | Elasticidad (log10) |
|---|---|---|---|---|
| Ingesta 1x -> 10x (batch y streaming, consumo base) | 20.32 | 24.20 | x1.19 | 0.08 |
| Ingesta 10x -> 100x (batch y streaming, consumo base) | 24.20 | 154.16 | x6.37 | 0.80 |
| Ingesta 100x -> 1000x (batch y streaming, consumo base) | 154.16 | 1,258.24 | x8.16 | 0.91 |
| Consumo x10 (8->80 actualiz./dia, 3->30 analistas), ingesta 1x | 20.32 | 184.03 | x9.06 | 0.96 |
| Consumo x10 (8->80 actualiz./dia, 3->30 analistas), ingesta 100x | 154.16 | 236.51 | x1.53 | 0.19 |
| Solo actualizaciones del tablero x10 (8->80/dia), ingesta 1x | 20.32 | 22.26 | x1.10 | 0.04 |
| Solo actualizaciones del tablero x10 (8->80/dia), ingesta 100x | 154.16 | 156.09 | x1.01 | 0.01 |

### Tabla 9. Modelo (escenario observado: 4.5 corridas en 30 dias) vs. Cost Explorer, costo bruto de uso 2026-10-09 a 2026-11-08 (USD)

| Servicio | Modelo | Cost Explorer | Diferencia |
|---|---|---|---|
| RDS (instancia + disco) | 13.82 | 13.76 | +0.06 |
| IPv4 publica (VPC) | 3.60 | 3.60 | -0.00 |
| Glue | 0.26 | 0.22 | +0.05 |
| Fargate (ECS) | 0.00 | 0.01 | -0.00 |
| S3 | 0.02 | 0.01 | +0.00 |
| Otros (Secrets, ECR, CloudWatch, etc.) | 0.83 | 0.41 | +0.42 |
| **Total** | **18.54** | **18.01** | **+0.52** |

### Tabla 10. Sensibilidad (+/-50 %): 6 supuestos con mayor efecto por escenario (cambio del costo mensual, USD)

| Escenario | Supuesto | -50 % | +50 % | Efecto medio / base |
|---|---|---|---|---|
| A. (base USD 20.32) | Precio de la instancia RDS | -5.76 | +5.76 | 28 % |
| A. (base USD 20.32) | Precio del DPU de Glue | -0.88 | +0.88 | 4 % |
| A. (base USD 20.32) | Corridas batch por dia | -0.79 | +0.79 | 4 % |
| A. (base USD 20.32) | Minutos fijos de Glue | -0.77 | +0.77 | 4 % |
| A. (base USD 20.32) | Volumen batch (GB/dia) | -0.17 | +0.17 | 1 % |
| A. (base USD 20.32) | Trabajo de Glue por GB | -0.11 | +0.11 | 1 % |
| B. (base USD 854.78) | Horas/mes que corre el streaming en AWS | -331.20 | +331.20 | 39 % |
| B. (base USD 854.78) | Precio del DPU de Glue | -330.50 | +330.50 | 39 % |
| B. (base USD 854.78) | Eventos/s de streaming | -72.28 | +26.20 | 6 % |
| B. (base USD 854.78) | Precio de la instancia RDS | -46.44 | +46.44 | 5 % |
| B. (base USD 854.78) | Volumen batch (GB/dia) | -19.90 | +20.01 | 2 % |
| B. (base USD 854.78) | Trabajo de Glue por GB | -13.31 | +13.31 | 2 % |
| C. (base USD 189.84) | Precio de la instancia RDS | -86.04 | +86.04 | 45 % |
| C. (base USD 189.84) | Analistas concurrentes | -79.76 | +0.56 | 21 % |
| C. (base USD 189.84) | CPU por analista (vCPU) | -79.20 | +0.00 | 21 % |
| C. (base USD 189.84) | Actualizaciones de tablero/dia | -5.08 | +6.48 | 3 % |
| C. (base USD 189.84) | GB por actualizacion del tablero | -5.08 | +6.48 | 3 % |
| C. (base USD 189.84) | Precio del DPU de Glue | -1.87 | +1.87 | 1 % |
