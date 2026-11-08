"""Reality check: costos reales de los ultimos 30 dias con Cost Explorer (solo lectura).

Hace 3 llamadas (cada una cuesta USD 0.01) y guarda cost_explorer.json:
  1. DAILY agrupado por SERVICE, solo RECORD_TYPE=Usage (costo bruto, sin creditos).
  2. DAILY agrupado por RECORD_TYPE (uso, credito, impuesto, ...), para ver si hay creditos que oculten costos.
  3. DAILY agrupado por USAGE_TYPE (detalle de que partidas componen el total).

Uso:  python cost_explorer_check.py
"""
import json
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

from aws_cli import aws

END = date.today()
START = END - timedelta(days=30)
TP = f"Start={START.isoformat()},End={END.isoformat()}"


def ce(group_key, usage_only):
    pages, token = [], None
    while True:
        args = ["ce", "get-cost-and-usage", "--region", "us-east-1", "--time-period", TP,
                "--granularity", "DAILY", "--metrics", "UnblendedCost",
                "--group-by", f"Type=DIMENSION,Key={group_key}"]
        if usage_only:  # excluye creditos e impuestos: costo bruto de uso
            args += ["--filter", '{"Dimensions":{"Key":"RECORD_TYPE","Values":["Usage"]}}']
        if token:
            args += ["--next-page-token", token]
        res = aws(*args)
        pages.append(res)
        token = res.get("NextPageToken")
        if not token:
            return pages


def totals(pages):
    t = defaultdict(float)
    for pg in pages:
        for day in pg["ResultsByTime"]:
            for g in day["Groups"]:
                t[g["Keys"][0]] += float(g["Metrics"]["UnblendedCost"]["Amount"])
    return {k: round(v, 4) for k, v in sorted(t.items(), key=lambda kv: -abs(kv[1]))}


def main():
    out = {"period": {"start": START.isoformat(), "end": END.isoformat(), "days": 30},
           "metric": "UnblendedCost", "currency": "USD"}
    for name, key, usage_only in (("by_service", "SERVICE", True), ("by_record_type", "RECORD_TYPE", False),
                                  ("by_usage_type", "USAGE_TYPE", True)):
        pages = ce(key, usage_only)
        out[name] = totals(pages)
        if name == "by_service":
            daily = defaultdict(float)
            for pg in pages:
                for day in pg["ResultsByTime"]:
                    daily[day["TimePeriod"]["Start"]] += sum(float(g["Metrics"]["UnblendedCost"]["Amount"]) for g in day["Groups"])
            out["daily_total"] = {k: round(v, 4) for k, v in sorted(daily.items())}
        print(name, json.dumps(out[name], indent=1))
    out["total_gross_usage"] = round(sum(out["by_service"].values()), 4)
    Path(__file__).with_name("cost_explorer.json").write_text(json.dumps(out, indent=2), encoding="utf-8")
    print("total", out["total_gross_usage"])


if __name__ == "__main__":
    main()
