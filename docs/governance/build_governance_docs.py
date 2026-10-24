"""Builds data_dictionary.md and quality_rules.md from dbt's own artifacts.

Run `dbt docs generate` in backend/src/dbt first (it writes target/manifest.json
and target/catalog.json against the warehouse), then from the repo root:

    python docs/governance/build_governance_docs.py backend/src/dbt/target

Column types come from catalog.json (what Postgres reports), descriptions and
tests from manifest.json (the schema.yml files and tests/). The sensitivity
class of each column is set in CLASSIFICATION below.
"""

import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent
DBT_PROJECT = OUT_DIR.parent.parent / "backend" / "src" / "dbt"

# Sensitivity classes. Anything not listed is "Interno".
DIRECT_PII = {"first_name", "last_name", "email", "phone_number", "address", "lat", "lon"}
QUASI_PII = {"date_of_birth", "gender", "nationality", "city", "age", "age_bucket"}
FINANCIAL = {
    "credit_score", "credit_score_band", "risk_score", "risk_tier", "bankruptcy_flag",
    "late_payments_12m", "inquiries_6m", "utilization_pct", "utilization_tier",
    "total_limit", "total_limit_usd", "total_used", "total_used_usd", "credit_info",
    "balance", "balance_usd", "credit_limit", "credit_limit_usd", "total_balance",
    "outstanding_balance", "outstanding_balance_usd", "days_past_due", "is_delinquent",
}
# Used only when no schema.yml in the project describes a column by that name.
GLOSSARY = {
    "date": "Transaction date as received, in one of four source formats; parsed by format_date in Silver.",
    "category": "Spending category of the transaction (groceries, rent, travel...); disguised nulls normalized.",
    "merchant": "Merchant name; NULL when the source sent an empty or disguised null.",
    "description": "Free-text description of the transaction; disguised nulls normalized.",
    "customer_country": "Country of the customer who made the transaction (ISO code), from dim_customer.",
    "is_international": "True when the transaction currency differs from the expected currency of the customer's country.",
    "amount_native": "Transaction amount in its own currency, after clean_amount.",
    "event_id": "Unique id of the Kafka event that carried the row.",
    "source": "Producer that generated the event (simulator for live transactions).",
    "event_time": "When the event happened, set by the producer (UTC).",
    "kafka_ts": "When Redpanda accepted the event (UTC).",
    "processed_at": "When the streaming job wrote the row (UTC); processed_at - event_time is the end-to-end latency.",
    "fx_event_time": "event_time of the FX tick joined to the transaction; NULL when no tick was in the window.",
    "fx_provider_updated_at": "When the joined FX price was actually set by its provider (trade time or API update).",
    "provider_updated_at": "When the provider set this price: the Binance trade time, or the daily API update time.",
    "rate_event_time": "event_time of the live tick behind usd_per_unit; NULL when the seed is used.",
    "seed_usd_per_unit": "Static rate from seed_fx_rates, kept to compare against the live one.",
    "base": "Base currency of the rate; always USD.",
    "units_per_usd": "Units of the currency for one USD.",
    "provider": "Where the rate came from: binance (market trades) or open.er-api.com (daily API).",
    "phone_number": "Phone number; normalized with the country prefix from seed_phone_codes in Silver.",
    "gender": "Customer gender as declared; disguised nulls normalized.",
    "nationality": "Customer nationality as declared.",
    "city": "Customer city, mapped to its canonical name through seed_city.",
    "address": "Street address of the customer.",
    "lat": "Latitude of the customer address.",
    "lon": "Longitude of the customer address.",
    "relationship_manager": "Bank employee assigned to the customer.",
    "credit_info": "Credit bureau object as JSON text; flattened into silver.credit_info.",
    "digital_engagement": "Digital channel usage object as JSON text; flattened into silver.digital_engagement.",
    "tenure_years": "Whole years since registration_date.",
    "registration_month": "First day of the registration month, for cohort analysis.",
    "total_limit": "Total credit limit across the customer's credit accounts, in USD in Gold.",
    "total_used": "Total credit used across the customer's credit accounts, in USD in Gold.",
    "oldest_account_age_months": "Age in months of the customer's oldest credit account.",
    "preferred_channel": "Channel the customer uses most (mobile, web, branch...).",
    "avg_monthly_logins": "Average digital logins per month.",
    "push_notifications": "True when push notifications are enabled.",
    "paperless_statements": "True when the customer receives statements digitally only.",
    "is_currency_mismatch": "True when the account currency differs from the expected currency of the customer's country.",
    "credit_limit": "Credit limit of the account; NULL for non-credit products.",
    "credit_limit_usd": "credit_limit converted to USD with to_usd.",
    "monthly_payment_usd": "monthly_payment converted to USD with to_usd.",
    "collateral_type": "Collateral backing the loan; NULL for unsecured loans.",
    "loan_type": "Loan product (mortgage, personal, auto...).",
    "day_name": "Day of the week name.",
    "week_number": "ISO week of the year.",
    "month_name": "Month name.",
    "country_code": "Country ISO code.",
    "phone_code": "International dialing prefix of the country.",
    "expected_currency": "Local currency expected for customers of that country.",
}
SUFFIX_GLOSSARY = {
    "_fuzzy": "Variant as it appears in the source (any case, Spanish, typos).",
    "_clean": "Canonical value the variant maps to.",
}

CLASS_LABEL = {
    "direct": "PII directa",
    "quasi": "PII indirecta",
    "financial": "Financiera sensible",
    "internal": "Interno",
}

LAYERS = [
    ("Fuentes (staging de Spark y streaming)", "source", "silver"),
    ("Silver (dbt)", "model", "silver"),
    ("Gold (dbt)", "model", "gold"),
    ("Seeds (tablas de referencia)", "seed", "seeds"),
]


def classify(column):
    if column in DIRECT_PII:
        return "direct"
    if column in QUASI_PII:
        return "quasi"
    if column in FINANCIAL:
        return "financial"
    return "internal"


def cell(text):
    return " ".join(str(text or "").split()).replace("|", "\\|")


def test_label(test):
    meta = test.get("test_metadata") or {}
    name = meta.get("name")
    if not name:
        return test["name"]
    kwargs = {k: v for k, v in (meta.get("kwargs") or {}).items() if k not in ("column_name", "model")}
    if name == "accepted_values":
        return f"accepted_values ({', '.join(map(str, kwargs.get('values', [])))})"
    if name == "relationships":
        to = re.sub(r"ref\('([^']+)'\)", r"\1", str(kwargs.get("to", "")))
        return f"relationships → {to}.{kwargs.get('field')}"
    if name == "accepted_range":
        return f"accepted_range [{kwargs.get('min_value')}, {kwargs.get('max_value')}]"
    return name


def singular_purpose(test, project_root):
    """First comment lines of a singular test's SQL file."""
    path = project_root / test["original_file_path"]
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("--"):
            break
        text = line.lstrip("- ").strip()
        if text and not text.lower().startswith("returns rows"):
            lines.append(re.sub(r"^Test:\s*", "", text))
    return " ".join(lines)


def main(target_dir):
    target = Path(target_dir)
    manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
    catalog = json.loads((target / "catalog.json").read_text(encoding="utf-8"))
    project_root = DBT_PROJECT

    nodes = {**manifest["nodes"], **manifest["sources"]}
    cat = {**catalog["nodes"], **catalog["sources"]}

    # Each test belongs to one model: the one its schema.yml entry sits on
    # (attached_node). A relationships test also depends on the parent table,
    # but it is that model's rule, not the parent's.
    tests_by_column = defaultdict(list)
    tests_by_model = defaultdict(list)
    for test in manifest["nodes"].values():
        if test["resource_type"] != "test":
            continue
        owner = test.get("attached_node") or test["depends_on"]["nodes"][0]
        tests_by_model[owner].append(test)
        if test.get("column_name"):
            tests_by_column[(owner, test["column_name"])].append(test)

    # A Gold column often repeats a Silver one under the same name; reuse the
    # first description found when the model's own schema.yml leaves it blank.
    fallback = {}
    for node in nodes.values():
        for name, col in (node.get("columns") or {}).items():
            if col.get("description") and name not in fallback:
                fallback[name] = col["description"]

    write_dictionary(nodes, cat, tests_by_column, fallback)
    write_quality_rules(nodes, tests_by_model, project_root)
    write_lineage(nodes)


def write_dictionary(nodes, cat, tests_by_column, fallback):
    totals = Counter()
    sections = []
    for title, resource_type, schema in LAYERS:
        selected = sorted(
            (uid for uid, n in nodes.items()
             if n["resource_type"] == resource_type and n.get("schema") == schema and uid in cat),
            key=lambda uid: nodes[uid]["name"],
        )
        if not selected:
            continue
        sections.append(f"## {title}\n")
        for uid in selected:
            node, meta = nodes[uid], cat[uid]
            kind = "vista" if meta["metadata"]["type"] == "VIEW" else "tabla"
            name = f"{node['schema']}.{node.get('identifier') or node['name']}"
            sections.append(f"### `{name}` ({kind})\n")
            if node.get("description"):
                sections.append(cell(node["description"]) + "\n")
            sections.append("| Columna | Tipo | Clasificación | Descripción | Tests |")
            sections.append("| --- | --- | --- | --- | --- |")
            described = node.get("columns", {})
            for col in sorted(meta["columns"].values(), key=lambda c: c["index"]):
                cname = col["name"]
                klass = classify(cname)
                totals[klass] += 1
                tests = ", ".join(sorted({test_label(t) for t in tests_by_column.get((uid, cname), [])}))
                desc = (
                    (described.get(cname) or {}).get("description")
                    or fallback.get(cname)
                    or GLOSSARY.get(cname)
                    or next((v for k, v in SUFFIX_GLOSSARY.items() if cname.endswith(k)), "")
                )
                sections.append(
                    f"| `{cname}` | {col['type']} | {CLASS_LABEL[klass]} | {cell(desc)} | {cell(tests)} |"
                )
            sections.append("")

    header = f"""# Diccionario de datos

Una entrada por columna de cada tabla del warehouse (`payload_analytics` en RDS PostgreSQL). El tipo es el que reporta Postgres; la descripción y los tests salen de los `schema.yml` de dbt. Se regenera con `build_governance_docs.py`.

**Dueño del dato:** Nicolás Ospina. Ver `raci.md`.

## Clasificación de sensibilidad

| Clase | Qué incluye | Columnas en el warehouse | Regla de uso |
| --- | --- | --- | --- |
| PII directa | Identifica a una persona por sí sola: nombre, correo, teléfono, dirección, coordenadas | {totals['direct']} | Solo en Silver y en `gold.dim_customer`. No se exporta ni se comparte fuera del equipo. Pendiente: enmascarar con hash SHA-256 antes de Gold. |
| PII indirecta | Identifica combinada con otros datos: fecha de nacimiento, género, nacionalidad, ciudad, edad | {totals['quasi']} | Reportar agregada (rangos de edad, país), nunca fila a fila con PII directa. |
| Financiera sensible | Score crediticio, riesgo, saldos, cupos, mora | {totals['financial']} | Uso analítico interno. Los reportes externos van agregados. |
| Interno | Identificadores técnicos, fechas, categorías, montos de transacción | {totals['internal']} | Sin restricción dentro del equipo. |

"""
    (OUT_DIR / "data_dictionary.md").write_text(header + "\n".join(sections), encoding="utf-8", newline="\n")
    print("data_dictionary.md:", dict(totals))


def write_quality_rules(nodes, tests_by_model, project_root):
    generic = Counter()
    rows_by_model = defaultdict(list)
    singular = []
    for uid, tests in tests_by_model.items():
        if uid not in nodes:
            continue
        for test in tests:
            meta = test.get("test_metadata") or {}
            if meta.get("name"):
                generic[meta["name"]] += 1
                rows_by_model[uid].append((test.get("column_name") or "", test_label(test)))
            else:
                singular.append(test)

    singular_unique = {t["unique_id"]: t for t in singular}.values()
    total = sum(generic.values()) + len(singular_unique)

    out = [f"""# Reglas de calidad implementadas

Hay **{total} reglas** que corren como tests de dbt en cada ejecución del pipeline (`dbt test`, dentro de `run.sh` en Fargate). Si una falla, la tarea de Fargate falla, Step Functions mueve el archivo a `failed/` y SNS envía un correo. Ninguna regla se debilita para que una corrida pase.

## Por tipo

| Tipo de regla | Qué verifica | Cantidad |
| --- | --- | --- |"""]
    meaning = {
        "not_null": "La columna nunca está vacía",
        "unique": "No hay duplicados (clave primaria)",
        "relationships": "La clave foránea existe en la dimensión",
        "accepted_values": "El valor está en la lista permitida",
        "accepted_range": "El valor está dentro de un rango",
        "is_non_negative": "El valor es mayor o igual a 0 (montos en USD)",
        "is_positive": "El valor es mayor que 0 (montos nativos)",
        "is_valid_date_format": "La fecha quedó en formato válido",
    }
    for name, count in generic.most_common():
        out.append(f"| `{name}` | {meaning.get(name, '')} | {count} |")
    out.append(f"| Singulares (reglas de negocio) | Consistencia entre columnas y capas | {len(singular_unique)} |")
    out.append("")

    out.append("## Reglas de negocio (tests singulares)\n")
    out.append("| Test | Capa | Regla |")
    out.append("| --- | --- | --- |")
    for test in sorted(singular_unique, key=lambda t: t["name"]):
        layer = "Gold" if test["name"].startswith("gold_") else "Silver"
        out.append(f"| `{test['name']}` | {layer} | {cell(singular_purpose(test, project_root))} |")
    out.append("")

    out.append("## Reglas por tabla\n")
    for uid in sorted(rows_by_model, key=lambda u: (nodes[u]["schema"], nodes[u]["name"])):
        node = nodes[uid]
        out.append(f"### `{node['schema']}.{node.get('identifier') or node['name']}`\n")
        out.append("| Columna | Regla |")
        out.append("| --- | --- |")
        for column, label in sorted(set(rows_by_model[uid])):
            out.append(f"| `{column}` | {cell(label)} |")
        out.append("")

    (OUT_DIR / "quality_rules.md").write_text("\n".join(out), encoding="utf-8", newline="\n")
    print("quality_rules.md:", total, "reglas")


def write_lineage(nodes):
    """Mermaid lineage: the steps before dbt (fixed) + every dbt edge from the manifest."""

    def mid(node):
        return f"{node['schema']}_{node.get('identifier') or node['name']}"

    def label(node):
        return f"{node['schema']}.{node.get('identifier') or node['name']}"

    dbt_nodes = {uid: n for uid, n in nodes.items() if n["resource_type"] in ("model", "source", "seed")}
    groups = {"stg": [], "stream": [], "seed": [], "silver": [], "gold": []}
    for n in dbt_nodes.values():
        if n["resource_type"] == "seed":
            groups["seed"].append(n)
        elif n["resource_type"] == "source":
            groups["stream" if n["name"].startswith("stream") else "stg"].append(n)
        else:
            groups["gold" if n["schema"] == "gold" else "silver"].append(n)

    lines = ["flowchart LR"]
    lines += [
        '  json["JSON clientes<br/>S3 raw/ (Bronze)"] --> glue["Glue Spark<br/>aplana y deduplica"]',
        '  binance["Binance WebSocket<br/>BRL ARS MXN COP EUR"] --> redpanda',
        '  erapi["open.er-api<br/>USD CLP PEN UYU"] --> redpanda',
        '  sim["Simulador de<br/>transacciones"] --> redpanda',
        '  redpanda["Redpanda<br/>fx.rates · fintech.live_transactions"] --> spark["Spark Structured<br/>Streaming"]',
        '  bi["Power BI"]',
    ]
    titles = {
        "stg": "Silver staging (Spark batch)",
        "stream": "Silver streaming (Spark)",
        "seed": "Seeds",
        "silver": "Silver (dbt)",
        "gold": "Gold (dbt)",
    }
    for key, members in groups.items():
        lines.append(f'  subgraph {key}["{titles[key]}"]')
        for n in sorted(members, key=label):
            lines.append(f'    {mid(n)}["{label(n)}"]')
        lines.append("  end")
    for n in groups["stg"]:
        lines.append(f"  glue --> {mid(n)}")
    for n in groups["stream"]:
        lines.append(f"  spark --> {mid(n)}")
    edges = set()
    for n in dbt_nodes.values():
        if n["resource_type"] != "model":
            continue
        for parent in n["depends_on"]["nodes"]:
            if parent in dbt_nodes:
                edges.add(f"  {mid(dbt_nodes[parent])} --> {mid(n)}")
    lines += sorted(edges)
    lines.append("  gold --> bi")

    text = f"""# Linaje de datos

Del dato crudo al reporte. El tramo antes de dbt (S3 → Glue y fuentes → Redpanda → Spark) es fijo; cada flecha entre tablas sale de las dependencias reales de dbt (`ref` y `source`, incluidas las que usan macros como `to_usd` → `fx_rates_current`), así que el diagrama cambia solo cuando cambian los modelos. Se regenera con `build_governance_docs.py`.

El linaje a nivel de columna y el SQL de cada modelo están en el catálogo de dbt: abre `catalog.html` en el navegador y usa el botón del grafo de linaje (abajo a la derecha).

```mermaid
{chr(10).join(lines)}
```

## Cómo leerlo

- **Batch:** un JSON en `raw/` dispara Glue, que escribe `silver.stg_*`. dbt limpia esas tablas en Silver y construye la estrella en Gold.
- **Streaming:** Binance, la API diaria y el simulador publican en Redpanda. Spark cruza cada transacción con su tasa y escribe `silver.stream_*`.
- **Donde se juntan:** `fx_rates_current` toma la última tasa del stream (o el seed si no hay ticks recientes), y todos los montos en USD del batch pasan por ella vía `to_usd`.
- **Seeds:** tablas de referencia versionadas en el repo (ciudades, estados, tipos, prefijos, monedas, tasas de respaldo).
"""
    (OUT_DIR / "lineage.md").write_text(text, encoding="utf-8", newline="\n")
    print("lineage.md:", len(edges), "dependencias dbt")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
