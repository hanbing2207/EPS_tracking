# -*- coding:utf-8 -*-


import mysql.connector
import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import re
import datetime as dt
import numpy as np
from sqlalchemy import create_engine



def get_last_date(stock):
    query = "SELECT date_ FROM db.eps\
                WHERE code_ = '%s'\
                ORDER BY date_ DESC\
                LIMIT 1;" % stock
    df = pd.read_sql(query, conn)
    if df.empty:
        return None
    else:
        last_date = df.iloc[0,0]
        return pd.to_datetime(last_date)



def get_pool():
    query = "SELECT code_ FROM db.stock_info"
    df = pd.read_sql(query, conn)
    return df["code_"]



def get_eps(links_dict, start_date):
    """
    get eps prediction
    :param stock: str
    :return: list of dicts [{"date":date
                            "eps": {2016:x.xx, 2017:x,xx ...},
                            {}...]
    """
    failed = 0
    n_try = 0
    results = []
    re_eps = re.compile(r".*(EPS|\xe6\xaf\x8f\xe8\x82\xa1\xe6\x94\xb6\xe7\x9b\x8a)[\x80-\xff\s]{0,14}(\d\.\d{1,2})[\x80-\xff\s，,、/]*(\d\.\d{1,2})[\x80-\xff\s，,、/]*(\d\.\d{2})")
    re_eps2 = re.compile(r".*(EPS|\xe6\xaf\x8f\xe8\x82\xa1\xe6\x94\xb6\xe7\x9b\x8a)[\x80-\xff\s]{0,14}(\d\.\d{1,2})[\x80-\xff\s，,、/]*(\d\.\d{1,2})")



    for dict in links_dict:
        author = dict["author"]
        url = dict["link"]
        eps_ref = dict["eps"]
        date = dict["date"]
        this_date = pd.to_datetime(date, format="%Y%m%d")
        if this_date < start_date:
            continue
        n_try += 1
        # print date,type(date),"\n\n"
        #skip existed data

        try:
            html = requests.get(url, headers, timeout=5)
        except Exception as e:
            print e
            continue
        if html.status_code == 404:
            continue
        html.encoding = "gb2312"
        soup = BeautifulSoup(html.text, "lxml")
        text = soup.find("div", class_="newsContent")
        paras = text.find_all("p")
        paras_count = range(len(paras))
        #check from the end of paras
        for i in paras_count:
            i = -i - 1
            para = paras[i].text
            match = [(x in para) for x in eps_ref]
            if match == [True]*len(match):
                break
            else:
                continue
        para = para.encode("utf-8")
        try:
            (start, end) = re.match(r".*(1\d)[\x80-\xff\-\s\~\d\/]{1,10}(1\d)\s{0,3}[Ee\s]{0,3}\xe5\xb9\xb4", para).groups()
            eps = re_eps.match(para).groups()[1:]
        except Exception as e:
            # if there are only two prediction:
            try:
                (start, end) = re.match(r".*(1\d)[\x80-\xff\-\s\~\d\/]{1,10}(1\d)\s{0,3}[Ee\s]{0,3}\xe5\xb9\xb4", para).groups()
                eps = re_eps2.match(para).groups()[1:]
            except Exception as e:
                failed += 1
                # print e
                # print url
                # print para,"\n"""
                continue
        (start, end) = (int("20"+start), int("20"+end))
        if len(eps) == 2:
            result = {"date": date, "eps": {start: float(eps[0]), end: float(eps[1])}, "author":author}
        else:
            mid = (start + end)/2
            result = {"date":date, "eps":{start:float(eps[0]), mid:float(eps[1]), end:float(eps[2])}, "author":author}
        # print result
        results.append(result)
    print "captured: %d/%d"%(n_try-failed, n_try)
    return results




def get_links(stock):
    """
    get links of reports of given stock
    :param stock: str
    :return: list of dict [{link: eps}], eps: list of eps
    """
    dicts = []
    page, pages = 1, 1
    while page <= pages:
        url = "http://datainterface.eastmoney.com//EM_DataCenter/js.aspx?type=SR&sty=GGSR&js=var%20HpjgOqgA={%22data%22:[(x)],%22pages%22:%22(pc)%22,%22update%22:%22(ud)%22,%22count%22:%22(count)%22}&ps=25&p=" \
              + str(page) + "&code=" + stock + "&rt=50505100"
        try:
            html = requests.get(url, headers,timeout=5)
            text = eval(html.text[13:])
        except NameError:
            print "No data for %s\n"%stock
            return None
        except Exception:
            print "get link time out!"
            return None
        js_str = json.dumps(text)
        js_data = json.loads(js_str)
        data = js_data["data"]
        number_of_reports = len(data)
        for i in range(number_of_reports):
            date = data[i]["datetime"]
            date = date[:4] + date[5:7] + date[8:10]
            eps = data[i]["sys"]
            #pop blank str
            j = 0
            while j < len(eps):
                if eps[j] == "":
                    eps.pop(j)
                    continue
                else:
                    j += 1
            infocode = data[i]["infoCode"]
            link = "http://data.eastmoney.com/report/" + date + "/" + infocode + ".html"
            author = data[i]["author"]
            dict = {"link":link, "eps":eps, "date":date, "author":author}
            dicts.append(dict)
        # when it's 1st page, check number of pages
        if page == 1:
            pages = float(js_data["pages"])
        page += 1
    print "links got!"
    return dicts



def transform2df(stock, data):

    dict1 = {2016:"2016eps", 2017:"2017eps", 2018:"2018eps", 2019:"2019eps", 2020:"2020eps"}
    list2 = ["author1", "author2", "author3", "author4"]
    rows = []

    for info in data:
        #form a row from each piece of info, then concate them
        row = pd.DataFrame([[np.nan]*11], columns=["date_", "code_", "2016eps", "2017eps", "2018eps", "2019eps", "2020eps", \
                               "author1", "author2", "author3", "author4"])
        row["date_"] = pd.to_datetime(info["date"]).date()
        row["code_"] = stock
        for eps in info["eps"]:
            columns_name = dict1[eps]
            row[columns_name] = info["eps"][eps]
        authors = info["author"].split(",")
        for i in range(len(authors)):
            name = authors[i]
            row[list2[i]] = name
        rows.append(row)
    df = pd.concat(rows)
    return df





        #
        #
        # row = pd.Series(data=[])
        # value = info["eps"]
        # author = info["author"]
        # value["author"] = author
        # s = pd.Series(data=value)
        # df = df.append(s)
        # df.index = pd.to_datetime(info["date"])
        # df = df.sort_index()


headers = {'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1"}
homedir = os.getenv("HOME") + "/"
pw = open(homedir + "Documents/mysql_pw.txt", "r").read()
engine = create_engine("mysql://root:" + pw + "@localhost:3306/db?charset=utf8", encoding="utf-8")
conn = mysql.connector.connect(user="root", password=pw, host="localhost")
cursor = conn.cursor()

stock_pool = get_pool()

z = 1
for stock in stock_pool:

    print "%d/%d: %s" % (z, len(stock_pool), str(stock))

    links = get_links(stock)
    if links == None:
        z += 1
        print "\n"
        continue

    last_update_date = get_last_date(stock)
    if last_update_date == None: last_update_date = dt.datetime(2016,1,1) #if record don't exist before, take this time

    start_date = last_update_date + dt.timedelta(1)  # the start date of this eps collection

    data = get_eps(links, start_date)
    if data == []:
        z += 1
        print "\n"
        continue

    df = transform2df(stock, data)
    df.to_sql("eps", engine, if_exists="append", index=False)
    print "wrote to sql\n"





    # # terminate outliers
    # # need to be done later!
    #
    # # write to file
    # df = df.rename(columns={2016: "16eps", 2017: "17eps", 2018: "18eps", 2019: "19eps"})
    # filename = "../data/db_eps/" + stock + "_EPS.csv"
    # dtype = {"16eps": float, "17eps": float, "18eps": float, "19eps": float, "author": str}
    # df_old = pd.read_csv(filename, index_col=0, dtype=dtype)
    # df_old.index = pd.to_datetime(df_old.index)
    # df = pd.concat([df_old, df])
    # # print df
    # df.to_csv(filename, encoding="utf-8")
    z += 1

