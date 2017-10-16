#!/usr/bin/env python

import os
import pickle
import re
import shutil
from datetime import date

import psycopg2
import simplejson
from smart_open import smart_open

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


def load_campaigns():
    result = {}
    split = re.compile("\[(\w+)\] Model(\d) Remi (\d+)(?:\-(\d+)|\+)")
    with open("nielsen_tags.json") as f:
        for row in f:
            r = simplejson.loads(row)
            name = r["label"]
            pixel = r["pixel"]
            for m in split.finditer(name):
                country, model, age_from, age_to = m.groups()
                if age_to is None:
                    age_to = "90"

                if model == "1":
                    file = "TRAIN_{}_{}_{}".format(country, age_from, age_to)
                    for age in range(2017 - int(age_to), 2017 - int(age_from) + 1):
                        result["TRAIN_{}_{}".format(country, age)] = [file, pixel]
                else:
                    file = "PREDICT_{}_{}_{}".format(country, age_from, age_to)
                    result["PREDICT_{}_{}_{}".format(country, 2017 - int(age_to), 2017 - int(age_from))] = [file, pixel]

    return result


def load_active_users():
    cur = conn.cursor()
    cur.execute("""
    select odid, most_frequent_country
    from users 
    where current_date <= last_update::date + 14
    --  and most_frequent_country in ('IT', 'FR', 'US', 'GB')
    and most_frequent_country = 'FR'
    """)

    r = dict(cur.fetchall())
    cur.close()

    return r


def process_train(fullname, campaigns, active_users, languages):
    print("Processing {}".format(fullname))

    with open(fullname, "r") as f:
        for row in f:
            s = row.rstrip().split("|")
            odid = s[0]
            if odid not in active_users:
                continue
            year_of_birth = int(s[-1])
            country = active_users.get(odid)
            k = "TRAIN_{}_{}".format(country, year_of_birth)
            if k in campaigns:
                file, pixel = campaigns[k]
                yield (dict(
                    odid=odid,
                    file=file,
                    pixel=pixel.format(**{
                        "aaid": odid,
                        "aaid-optout": 0,
                        "timestamp": """`date +%s`"""
                    }),
                    language=languages[country],
                ))


def process_predict(fullname, campaigns, active_users, languages):
    print("Processing {}".format(fullname))
    for row in smart_open(fullname, mode="rb"):
        odid, _, date_from, date_to, score = row.decode("utf-8").rstrip().split("\t")
        if odid not in active_users:
            continue

        country = active_users.get(odid)
        score = float(score)

        if score < 0.7 or score == 1:
            continue

        k = "PREDICT_{}_{}_{}".format(active_users[odid], date_from, date_to)

        if k in campaigns:
            file, pixel = campaigns[k]
            yield (dict(
                odid=odid,
                file=file,
                pixel=pixel.format(**{
                    "aaid": odid,
                    "aaid-optout": 0,
                    "timestamp": """`date +%s`"""
                }),
                language=languages[country],
            ))


def main():
    if os.path.exists("test_results"):
        shutil.rmtree("test_results")
    os.makedirs("test_results")

    campaigns = load_campaigns()
    active_users = load_active_users()
    languages = {"FR": "fr-FR", "IT": "it-IT", "US": "en-US", "GB": "en-GB"}

    # TRAIN

    dedup = set()
    count = {}
    fhs = {}

    p = "recommender-user-profile/training"
    for name in sorted(os.listdir(p), reverse=True):
        fullname = "{}/{}".format(p, name)
        if os.path.isfile(fullname):
            for f in process_train(fullname, campaigns, active_users, languages):
                if f["odid"] in dedup:
                    continue
                else:
                    dedup.add(f["odid"])

                if f["file"] not in fhs:
                    count[f["file"]] = 0
                    fhs[f["file"]] = open("test_results/{}".format(f["file"]), "w")

                if count[f["file"]] == 1500:
                    continue

                count[f["file"]] += 1
                print(
                    """curl -L -s -o /dev/null -A "Mozilla/5.0 (Linux; Android 6.0.1; Redmi Note 3 Build/MOB30Z; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.106 Mobile Safari/537.36" -H "Accept: image/webp,image/*,*/*;q=0.8" -H "Referer: http://www.litecdn.com/8dd755c/formats/webviews/multiwebviews/percent.html" -H "Accept-Encoding: gzip, deflate" -H "Accept-Language: {language},en-US;q=0.8" -H "X-Requested-With: co.ogury.ogurytestapp.app1" "{pixel}" """
                        .format(
                        pixel=f["pixel"], language=f["language"]
                    ), file=fhs[f["file"]])
                print("sleep 1", file=fhs[f["file"]])
            for f in fhs.values():
                f.flush()

    for f in fhs.values():
        f.close()

    # PREDICT
    count = {}
    fhs = {}

    p = "recommender-user-profile/predict"
    for name in sorted(os.listdir(p), reverse=True):
        fullname = "{}/{}".format(p, name)
        if os.path.isfile(fullname):
            for f in process_predict(fullname, campaigns, active_users, languages):
                if f["file"] not in fhs:
                    count[f["file"]] = 0
                    fhs[f["file"]] = open("test_results/{}".format(f["file"]), "w")

                if count[f["file"]] == 1500:
                    continue

                count[f["file"]] += 1
                print(
                    """curl -L -s -o /dev/null -A "Mozilla/5.0 (Linux; Android 6.0.1; Redmi Note 3 Build/MOB30Z; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/51.0.2704.106 Mobile Safari/537.36" -H "Accept: image/webp,image/*,*/*;q=0.8" -H "Referer: http://www.litecdn.com/8dd755c/formats/webviews/multiwebviews/percent.html" -H "Accept-Encoding: gzip, deflate" -H "Accept-Language: {language},en-US;q=0.8" -H "X-Requested-With: co.ogury.ogurytestapp.app1" "{pixel}" """
                        .format(
                        pixel=f["pixel"], language=f["language"]
                    ), file=fhs[f["file"]])
                print("sleep 1", file=fhs[f["file"]])
            for f in fhs.values():
                f.flush()

    for f in fhs.values():
        f.close()


if __name__ == "__main__":
    main()
