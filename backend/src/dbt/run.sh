#!/bin/bash
set -e
dbt seed
dbt run
dbt test
