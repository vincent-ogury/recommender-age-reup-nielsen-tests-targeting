aws s3 sync s3://ogury-tmp/vincent/recommender-user-profile/data/ recommender-user-profile
cd recommender-user-profile
for i in dt\=????-??-??
do
  if [ ! -f "$i".csv ]
  then
    echo "$i"
    gzcat "$i"/*.gz | perl -nE '/\|\d{4}\r/ && print' > "$i".csv
  fi
done
