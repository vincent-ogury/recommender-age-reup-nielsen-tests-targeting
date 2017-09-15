#aws s3 sync s3://ogury-tmp/vincent/recommender-user-profile/data/ recommender-user-profile
#aws s3 rm --rec s3://ogury-tmp/vincent/recommender-user-profile/
aws s3 sync s3://ogury-recommender-prod/recommender-user-profile/data/ recommender-user-profile/training/
cd recommender-user-profile/training/
for i in dt\=????-??-??
do
  if [ ! -f "$i".csv ]
  then
    echo "$i"
    gzcat "$i"/*.gz | perl -nE '/\|\d{4}\r/ && print' > "$i".csv
  fi
done
cd ../..
dt=`date -u +%Y-%m-%d`
aws s3 sync --del s3://ogury-recommender-prod/recommender-age/predict/xgboost/multi_softprob/dt=${dt}/ recommender-user-profile/predict/
