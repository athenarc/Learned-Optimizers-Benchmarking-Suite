#!/bin/bash
set -ex

DATA_DIR=$1
DBNAME=${2:-ssb}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
$PSQL $DBNAME -f "$DIR/schema.sql"

pushd $DATA_DIR
$PSQL $DBNAME -c "\copy part from '$1/part.tbl' delimiter '|' csv header escape '\\'" &
$PSQL $DBNAME -c "\copy customer from '$1/customer.tbl' delimiter '|' csv header escape '\\'" &
$PSQL $DBNAME -c "\copy date from '$1/date.tbl' delimiter '|' csv header escape '\\'" &
$PSQL $DBNAME -c "\copy lineorder from '$1/lineorder.tbl' delimiter '|' csv header escape '\\'" &
$PSQL $DBNAME -c "\copy supplier from '$1/supplier.tbl' delimiter '|' csv header escape '\\'" &
wait
popd