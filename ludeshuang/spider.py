# coding: utf8

"""
豆瓣小组爬虫
"""

import time
import random
import logging
import requests
from lxml import etree
import re
import pymongo

import mysql.connector

conn = mysql.connector.connect(host='localhost', user='root',passwd='',db='myDB')
cursor = conn.cursor()


group_pool = [
"https://www.douban.com/group/haixiuzu/discussion?start=0",
"https://www.douban.com/group/510760/discussion?start=0",
"https://www.douban.com/group/515085/discussion?start=0",
"https://www.douban.com/group/bw0766/discussion?start=0"
]

p = re.compile("<a href=\"(.*)\"\s*title=(.*)class=.*>(.*)</a>\s*</td>\s*<td\s*nowrap=.*><a href=\"(.*)\" class=.*>(.*)</a></td>")
pp = re.compile("<div class=\"topic-figure cc\">\s*<img src=\"(.*)\" alt=\"\" class=\"\">")

client = pymongo.MongoClient("localhost", 27017)
db = client.douban_url
collection = db.test_collection

def main():
    while True:
        print 'begin...'
        headers = {'User-Agent': 'AppleWebKit/537.36 (KHTML, like Gecko)'}
        for starturl in group_pool: 
            resp = requests.get(starturl, headers = headers)
            resp = resp.content.decode("utf8")
            print 'get web done'
            results = p.findall(resp)
            for result in results:
                url_2 = result[0]
                webid = url_2.split('/')[-2]
                #x = url_2.replace(".","")
                sql = "select url from douban_url_spider where url = %s" % webid
                print sql
                cursor.execute(sql)
                alldata = cursor.fetchall()
                if len(alldata) != 0:
                    print "%s already in base" % webid
                    continue

                resp_2 = requests.get(url_2, headers = headers)
                resp_2 = resp_2.content.decode("utf8")
                result2 = pp.findall(resp_2)

                for k in result2:
                    name = k.split('/')[-1]
                    f = open("data/" + name ,"wb")
                    data = requests.get(k).content
                    f.write(data)
                    f.close()
                    print k
                    time.sleep(5)

                sql = "insert into douban_url_spider (url, timestamp)  values (%s,%d)" % (webid, int(time.time()))
                print sql
                cursor.execute(sql)
                conn.commit()
                time.sleep(5)
                #break
            time.sleep(5)


if __name__ == "__main__":
    main()
