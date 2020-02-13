# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import pandas as pd
class AmazonPipeline(object):
    host = "localhost"
    database = "sj"
    user = "root"
    password = "xbyte123"
    port = 3306
    insert_count = 0
    con1 = pymysql.connect(host, user, password, database)

    def __init__(self):
        try:
            print("============Init Try Called=========")
            self.con1.cursor().execute("CREATE DATABASE IF NOT EXISTS " + self.database)
            self.con  = pymysql.connect(self.host,self.user,self.password,self.database,self.port)
            self.cursor = self.con.cursor()

            table = "CREATE TABLE amazon (id INT(6) UNSIGNED AUTO_INCREMENT PRIMARY KEY,name VARCHAR(300) NOT NULL,price VARCHAR(300) NOT NULL,imgurl VARCHAR(300),rating VARCHAR(300))"
            self.cursor.execute(table)
        except Exception as e:
            print("==========Table Create Exception==========")

    def process_item(self, item, spider):
        try:
            con = pymysql.connect(self.host,self.user,self.password,self.database,self.port)
            cursor = con.cursor()
            query = ("insert into amazon(name,price,imgurl,rating)values(%s,%s,%s,%s)")
            value = (item['name'],item['price'],item['image'],item['rating'])
            cursor.execute(query,value)
            con.commit()
            self.insert_count = self.insert_count+1
            print("Data Inserted----------->"+str(self.insert_count))
        except Exception as e2:
            print("================Exception Table Inserted=============",e2)
        return item




    def close_spider(self,spider):
        try:
            con = pymysql.connect(self.host,self.user,self.password,self.database,self.port)
            cursor = con.cursor()
            sql = f"select * from amazon"
            df = pd.read_sql(sql, con)
            df.to_csv('F:\\Sunit\\scrapy\\Amzon\\amazon\\CSV\\output.csv', index=False)
            print('================file genarated : ==============')
        except Exception as e:
            print("==========FILE EXCEPTION=======",e)
