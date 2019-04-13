import pymongo
import pymysql

from config import *
from sql import *


class Export():
    def __init__(self):
        self.mysql_client = pymysql.connect(host=mysql_url, user=mysql_user, password=mysql_password)
        self.mysql_cursor = self.mysql_client.cursor()
        self.mongo_client = pymongo.MongoClient(host=mongo_url)
        self.mongo_collection = self.mongo_client[mongo_database][mongo_collection]
        self.create_database_sql = create_database_sql % mysql_database

    def create_database(self):
        try:
            self.mysql_cursor.execute(self.create_database_sql)
        except Exception as e:
            print("creating database %s errors %s" % (mysql_database, str(e)))

    def get_table_structure(self):
        field_list = list(self.mongo_collection.find_one().keys())
        field_list.remove('_id')
        field_sql = ''
        for i in field_list:
            field_sql = field_sql + "%s VARCHAR(100)," % i
        field_sql = field_sql.strip(',')
        self.create_table_sql = create_table_sql % (mongo_collection,
                                                    "id VARCHAR(100) PRIMARY KEY," + field_sql)

    def create_table(self):
        try:
            self.mysql_cursor.execute(use_database % mysql_database)
            self.mysql_cursor.execute(self.create_table_sql)
        except Exception as e:
            print('creating table %s errors %s' % (mongo_collection, str(e)))

    def get_mongo_data(self):
        pass

    def export_data(self):
        datas = self.mongo_collection.find()
        for item in datas:
            field = ''
            value = ''
            for k, v in item.items():
                if k == '_id':
                    field = field + 'id' + ','
                else:
                    field = field + str(k) + ','
                if type(v) == list:
                    if v:
                        value = value + '"' + ','.join(v) + '"' + ','
                    else:
                        value = value + '"' + ' ' + '"' + ','
                else:
                    value = value + '"' + str(v) + '"' + ','
            field = field.rstrip(',')
            value = value.rstrip(',')
            try:
                # print(insert_one_sql % (mongo_collection, field, value))
                self.mysql_cursor.execute(insert_one_sql % (mongo_collection, field, value))
            except Exception as e:
                print('errors! %s inserting data %s ' % (str(item), str(e)))
            else:
                self.mysql_client.commit()
                print('success! inserting data %s ' % (str(item)))

    def close(self):
        self.mysql_cursor.close()
        self.mysql_client.close()
        self.mongo_client.close()

    def main(self):
        self.create_database()
        self.get_table_structure()
        self.create_table()
        self.export_data()
        self.close()


if __name__ == '__main__':
    e = Export()
    e.main()
