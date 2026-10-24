#!/bin/bash
# Uso: ./run_explain.sh <nombre> <archivo_consulta.sql>
# Corre la consulta 4 veces con EXPLAIN (ANALYZE, BUFFERS). La corrida 1 es "fria".
set -e
NAME=$1; Q=$2; OUT="resultados/${NAME}.txt"
{
  echo "-- fecha: $(date -u +%FT%TZ)"
  echo "-- $(psql -Atc 'select version()')"
  for i in 1 2 3 4; do
    echo "=== corrida $i ==="
    psql -v ON_ERROR_STOP=1 -c "SET max_parallel_workers_per_gather = 0" -c "EXPLAIN (ANALYZE, BUFFERS) $(cat "$Q")"
  done
} > "$OUT"
echo "Guardado en $OUT"; grep -E "corrida|Execution Time" "$OUT"
