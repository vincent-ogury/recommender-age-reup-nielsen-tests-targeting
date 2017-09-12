#!/usr/bin/env python

import os
import pickle
import re
import shutil
import time
from datetime import date

import psycopg2
import simplejson

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

            for age in range(2017 - int(age_to), 2017 - int(age_from) + 1):
                result["{}_{}_{}".format(model, country, age)] = dict(
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


@cache("pixel")
def load_pixel():
    pixel = {}
    for name in os.listdir("exports"):
        with open("exports/{}".format(name), "r") as f:
            r = simplejson.load(f)
            id = int(r["id"])
            for d in r["params"]:
                if d["name"] == "display_trackers":
                    p = d["value"]
                    pixel[id] = p.split('"')[1].replace("[", "{").replace("]", "}")
    return pixel


def process(fullname, campaigns, active_users, pixels, languages):
    print("Processing {}".format(fullname))

    with open(fullname, "r") as f:
        for row in f:
            s = row.rstrip().split("|")
            odid = s[0]
            year_of_birth = int(s[-1])
            country = active_users.get(odid)
            k = "TRAIN_{}_{}".format(country, year_of_birth)
            if k in campaigns:
                f = campaigns[k]
                pixel = pixels[f['id']]
                yield (dict(
                    odid=odid,
                    pixel=pixel.format(**{
                        "aaid": odid,
                        "aaid-optout": 0,
                        "timestamp": """`date +%s`"""
                    }),
                    language=languages[country],
                    **f
                ))

                # for odid, date_from, date_to in train:
                #     if odid in active_users:
                #         k = "TRAIN_{}_{}_{}".format(active_users[odid], 2017 - int(date_to), 2017 - int(date_from))
                #         if k in campaigns:
                #             f = campaigns[k]
                #             yield dict(odid=odid, **f)


def main():
    if os.path.exists("test_results"):
        shutil.rmtree("test_results")
    os.makedirs("test_results")

    p = "recommender-user-profile"

    campaigns = load_campaigns()
    active_users = load_active_users()
    pixel = load_pixel()
    languages = {"FR":"fr-FR", "IT": "it-IT", "US": "en-US", "GB": "en-GB"}

    dedup = set()
    count = {}
    fhs = {}
    for name in sorted(os.listdir(p), reverse=True):
        fullname = "{}/{}".format(p, name)
        if os.path.isfile(fullname):
            for f in process(fullname, campaigns, active_users, pixel, languages):
                if f["odid"] in dedup:
                    continue
                else:
                    dedup.add(f["odid"])

                if f["file"] not in fhs:
                    count[f["id"]] = 0
                    fhs[f["file"]] = open("test_results/{}".format(f["file"]), "w")

                if count[f["id"]] == 1500:
                    continue

                count[f["id"]] += 1
                print("""curl -s -o /dev/null -A "Mozilla/5.0 (Linux; Android 6.0.1; Redmi Note 3 Build/MOB30Z; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.106 Mobile Safari/537.36" -H "Accept: image/webp,image/*,*/*;q=0.8" -H "Referer: http://www.litecdn.com/8dd755c/formats/webviews/multiwebviews/percent.html" -H "Accept-Encoding: gzip, deflate" -H "Accept-Language: {language},en-US;q=0.8" -H "X-Requested-With: co.ogury.ogurytestapp.app1" "{pixel}" """
                      .format(
                    pixel=f["pixel"], language=f["language"]
                ), file=fhs[f["file"]])

    for f in fhs.values():
        f.close()


if __name__ == "__main__":
    main()
