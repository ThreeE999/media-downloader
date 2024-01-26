# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import os
import sqlite3
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline



class SqlitePipeline:
    db_path: str

    def __init__(self, db_path=None):
        if db_path is not None:
            self.db_path = db_path
        elif not getattr(self, "db_path", None):
            raise ValueError(f"{type(self).__name__} must have a db_path")
        directory_path = os.path.dirname(self.db_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)

    def open_spider(self, spider):
        self.connect = sqlite3.connect(self.db_path)
        self.cursor = self.connect.cursor()
        self.build()

    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

    def build(self):
        raise NotImplementedError(
            f"{self.__class__.__name__}.build is not defined"
        )

    def insert(self, table: str, data: dict):
        sql = f"""
            INSERT INTO `{table}` ({','.join(data.keys())})
            VALUES ({', '.join(['?']*len(data))});
        """
        self.cursor.execute(sql, tuple(data.values()))
        self.connect.commit()
