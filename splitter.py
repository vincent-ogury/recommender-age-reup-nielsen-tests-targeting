#!/usr/bin/env python

import os
import re
import shutil
import psycopg2
import pickle
from datetime import date
import boto
from smart_open import smart_open
from multiprocessing import Pool, cpu_count

active_users = {}

conn = psycopg2.connect(dbname="dwurlvisits", host="ogurydatawarehouse.cikxp3g7d0py.eu-west-1.redshift.amazonaws.com",
                        port=5439, user="vincent", password="K4XbY4Tso9qvm6Zh")


def cache(name):
    d = ".cache/{}".format(date.today())
    if not os.path.exists(d):
        os.makedirs(d)

    def decorated(func):
        def wrapper(*args, **kwargs):
            cache = "{}/{}.cache".format(d, name)
            if os.path.isfile(cache):
                with open(cache, "rb") as f:
                    print("Loading cache {} ...".format(name))
                    return pickle.load(f)
            print("Loading redshift {} ...".format(name))
            res = func(*args, **kwargs)
            with open(cache, "wb") as f:
                pickle.dump(res, f)
            return res

        return wrapper

    return decorated


@cache("campaigns")
def load_campaigns():
    cur = conn.cursor()
    cur.execute(
        "select campaign_id, campaign_name from campaigns where campaign_id >= 27746 and campaign_name like '%Remi%' and campaign_name not like 'OLD%'")
    result = {}
    split = re.compile("\[(\w+)\] Model(\d) Remi (\d+)(?:\-(\d+)|\+)")
    for id, name in cur.fetchall():
        for m in split.finditer(name):
            country, model, age_from, age_to = m.groups()
            if age_to is None:
                age_to = "90"

            if model == "1":
                model = "TRAIN"
            else:
                model = "PREDICT"

            result["{}_{}_{}_{}".format(model, country, age_from, age_to)] = dict(
                file="REMI_{}_{}_{}_{}_{}".format(id, country, age_from, age_to, model),
                id=id
            )

    cur.close()

    return result


@cache("active_users")
def load_active_users():
    cur = conn.cursor()
    cur.execute("""
    select odid, most_frequent_country
    from users 
    where current_date <= last_update::date + 14
      and most_frequent_country in ('IT', 'FR', 'US', 'GB')
    """)

    r = dict(cur.fetchall())
    cur.close()

    return r


@cache("train")
def load_train():
    cur = conn.cursor()
    cur.execute("""
    drop table if exists categories;
create temp table categories
(
  lower_year int,
  upper_year int
);

insert into categories values
(2000, 2004),
(1997, 1999),
(1993, 1996),
(1988, 1992),
(1983, 1987),
(1978, 1982),
(1973, 1977),
(1968, 1972),
(1963, 1967),
(1953, 1962),
(1927, 1952);

select odid, lower_year, upper_year
from
(
  select odid, date_part(year, year_of_birth):: int as year
  from data_science.recommender_user_profile rup inner join users u
using(odid)
  where current_date between rup.start_date and rup.end_date
  and current_date <= u.last_update::date + 7
  and year_of_birth is not null
) x inner join categories cat on x.year between cat.lower_year and cat.upper_year
""")

    r = cur.fetchall()
    cur.close()
    return r


def process_train(train, campaigns, active_users):
    print("Processing train ...")
    for odid, date_from, date_to in train:
        if odid in active_users:
            k = "TRAIN_{}_{}_{}".format(active_users[odid], 2017 - int(date_to), 2017 - int(date_from))
            if k in campaigns:
                f = campaigns[k]
                yield dict(odid=odid, **f)


def process_predict(s3connect, campaigns, active_users, s3bucket, s3key):

    r = []
    for row in smart_open(s3connect.get_bucket(s3bucket).get_key(s3key, validate=False), mode="r"):
        odid, _, date_from, date_to, score = row.rstrip().split("\t")
        score = float(score)
        if odid not in active_users:
            continue

        if score < 0.7 or score == 1:
            continue

        k = "PREDICT_{}_{}_{}".format(active_users[odid], 2017 - int(date_to), 2017 - int(date_from))

        if k in campaigns:
            f = campaigns[k]
            yield dict(odid=odid, **f)


def main():
    if os.path.exists("results"):
        shutil.rmtree("results")
    os.makedirs("results")

    campaigns = load_campaigns()
    active_users = load_active_users()

    fhs = {}

    # TRAIN
    for f in process_train(load_train(), campaigns, active_users):
        if f["file"] not in fhs:
            fhs[f["file"]] = open("results/{}".format(f["file"]), "w")
        print("{odid}|{id}|Branding|1000000".format(odid=f["odid"], id=f["id"]), file=fhs[f["file"]])

    for f in fhs.values():
        f.close()

    # PREDICT
    fhs = {}
    s3connect = boto.connect_s3()
    s3bucket = "ogury-recommender-prod"
    s3path = "recommender-age/predict/xgboost/multi_softprob/dt={}".format(date.today())

    to_process = [
        v.key
        for v in s3connect.get_bucket(s3bucket).list(s3path)
        if v.key.endswith(".gz")
    ]

    print("Processing predict ...")
    for s3key in to_process:
        print("        s3://{}/{}".format(s3bucket, s3key))
        for f in process_predict(s3connect, campaigns, active_users, s3bucket, s3key):
            if f["file"] not in fhs:
                fhs[f["file"]] = open("results/{}".format(f["file"]), "w")
            print("{odid}|{id}|Branding|1000000".format(odid=f["odid"], id=f["id"]), file=fhs[f["file"]])
        for f in fhs.values():
            f.flush()

    for f in fhs.values():
        f.close()


if __name__ == "__main__":
    main()
