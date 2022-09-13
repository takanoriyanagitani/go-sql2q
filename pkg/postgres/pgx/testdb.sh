#!/bin/bash

ITEST_SQL2Q_PGX_DBNAME=itest_sql2q_pgx
img=postgres:14.5-alpine3.16
name=$ITEST_SQL2Q_PGX_DBNAME

sockdir=$PWD/test.d/sock.d

mkdir -p $sockdir

docker rm --force $name

docker \
  run \
  --name $name \
  --detach \
  --env POSTGRES_PASSWORD=postgres \
  --env PGDATA=/pgdata/data \
  --env TZ=Etc/UTC \
  --volume $sockdir:/var/run/postgresql \
  $img

export PGUSER=postgres
export PGHOST=$sockdir

echo waiting db...
while ( pg_isready --timeout 60 1>/dev/null 2>/dev/null && echo ok || echo ng ) | fgrep -q ng; do
	sleep 1
done

echo "CREATE DATABASE $ITEST_SQL2Q_PGX_DBNAME" | psql
