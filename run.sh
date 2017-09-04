#!/bin/bash
set -e

[ -d ".cache/`date +%Y-%m-%d`" ] || rm -rf .cache

time python splitter.py

aws s3 rm --recursive s3://ogury-tmp/vincent/recommender-age/results/
aws s3 cp --recursive results/ s3://ogury-tmp/vincent/recommender-age/results/

for i in `seq 0 30`
do
  [ "$(date -j -v+${i}d +%m)" != "09" ] && break
  d=$(date -j -v+${i}d +%Y-%m-%d)
  echo aws s3 cp --recursive s3://ogury-tmp/vincent/recommender-age/results/ s3://ogury-ods/prod/users_external/dt=${d}/
done | parallel -u

