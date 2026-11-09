# Matriz RACI

Quién hace, quién responde y a quién se consulta o informa en cada actividad del ciclo de vida del dato.

**R** = responsable (lo hace) · **A** = aprobador (responde por el resultado; uno por fila) · **C** = consultado antes de decidir · **I** = informado del resultado.

**Dueño del dato:** Nicolás Ospina. Es el aprobador de todo lo que cambia cómo se captura, guarda, transforma o expone el dato de clientes y transacciones.

| Actividad | Nicolás Ospina | Samuel Deossa | Nicolás Saldarriaga | Samuel Henao |
| --- | --- | --- | --- | --- |
| Infraestructura AWS como código (S3, RDS, ECR, alertas) | **R/A** | C | I | I |
| Pipeline batch (Glue, dbt en Fargate, Step Functions) | **R/A** | C | I | I |
| Pipeline de streaming (Redpanda, productores FX y transacciones, Spark) | A | **R** | C | I |
| Modelado dbt Silver y Gold, conversión a USD con tasa en vivo | A | **R** | C | C |
| Optimización de la base de datos (índices, mediciones `EXPLAIN`) | A | C | **R** | I |
| Reglas de calidad (tests de dbt) | A | **R** | C | **R** |
| Gobernanza: diccionario, catálogo, linaje, clasificación de sensibilidad | A | C | I | **R** |
| Seguridad y acceso (secretos, cuenta AWS compartida, datos personales) | **R/A** | C | I | C |
| CI/CD y despliegue (GitHub Actions, rol OIDC, pruebas en CI) | A | **R** | C | I |
| Monitoreo y alertas (dashboard y alarmas de CloudWatch, métricas del streaming) | A | **R** | I | I |
| Costo y escalabilidad (modelo de costo 1×, 10×, 100×) | A | **R** | C | I |
| Consumo: dashboard en Power BI | A | C | I | **R** |
| Documentación del repositorio (README, CHANGELOG) | **R/A** | **R** | **R** | C |
| Presentación del sprint | A | C | C | **R** |

## Reglas de decisión

- **Un cambio de esquema** en `silver` o `gold` (migración de yoyo o columna nueva en un modelo) necesita la aprobación de Nicolás Ospina antes de desplegarse.
- **Un despliegue a AWS** se avisa antes en el grupo. La cuenta es compartida y los stacks no llevan el nombre del desarrollador: usar `DEVELOPER=nico11`.
- **Un test de calidad que falla** no se debilita para que la corrida pase. Quien es R de esa capa corrige el dato o el modelo.
- **Un cambio en la clasificación de sensibilidad** de una columna se decide con el responsable de gobernanza (Samuel Henao) y lo aprueba el dueño del dato.
- **Todo cambio entra a `main` por PR** revisado por al menos otro integrante.
- **Un merge a `main` despliega solo.** El job `deploy` aplica la migración, sube la imagen de dbt y actualiza la state machine; por eso se agrupan los cambios en un PR y no se fusiona mientras haya una corrida del pipeline o una demo en curso.
