# Gobernanza y calidad del dato

| Documento | Qué contiene |
| --- | --- |
| [data_dictionary.md](data_dictionary.md) | Diccionario de datos: las 375 columnas del warehouse con tipo, descripción, clasificación de sensibilidad y tests. |
| [catalog.html](catalog.html) | Catálogo de datos navegable (dbt docs): tablas, columnas, SQL de cada modelo y grafo de linaje. Se abre en cualquier navegador, sin servidor. |
| [lineage.md](lineage.md) | Linaje de punta a punta, del JSON en S3 y las fuentes de streaming hasta Gold y Power BI. |
| [quality_rules.md](quality_rules.md) | Las 206 reglas de calidad implementadas como tests de dbt, por tipo y por tabla. |
| [raci.md](raci.md) | Matriz RACI y reglas de decisión. Dueño del dato: Nicolás Ospina. |

## Estado de la gobernanza

- **Calidad:** 206 reglas corren en cada ejecución del pipeline. Si una falla, se detiene el pipeline y llega un correo (Step Functions → `failed/` → SNS).
- **Clasificación:** cada columna tiene clase de sensibilidad. Hay 17 columnas de PII directa, 16 de PII indirecta y 43 financieras sensibles.
- **Secretos:** la contraseña de la base vive solo en AWS Secrets Manager. Glue, Fargate y el job de streaming la leen de ahí; no está en el repositorio ni en archivos `.env`.
- **Pendiente:** la PII directa llega sin enmascarar hasta `gold.dim_customer`. El plan del Sprint 1 era aplicar hash SHA-256 en Silver y crear vistas con acceso por rol sobre Gold; sigue abierto.

## Regenerar

El diccionario, las reglas y el linaje salen de los artefactos de dbt, así que se actualizan solos cuando cambian los modelos:

```bash
cd backend/src/dbt && dbt docs generate --static
cd ../../.. && python docs/governance/build_governance_docs.py backend/src/dbt/target
cp backend/src/dbt/target/static_index.html docs/governance/catalog.html
```

`dbt docs generate` necesita las variables `DBT_*` de `profiles.yml` apuntando al RDS. Solo lee el catálogo de Postgres; no escribe nada.
