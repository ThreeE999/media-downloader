# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import logging
import os
import sqlite3
import enlighten
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline, FileException
from scrapy.exceptions import DropItem
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.request import referer_str
from scrapy.settings import Settings

logger = logging.getLogger(__name__)

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
        spider.cursor = self.cursor
        spider.connect = self.connect
        spider.db_path = self.db_path

    def process_item(self, item, spider):
        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.connect.close()

    def insert(self, table: str, data: dict):
        sql = f"""
            INSERT OR REPLACE INTO `{table}` ({','.join(data.keys())})
            VALUES ({', '.join(['?']*len(data))});
        """
        self.cursor.execute(sql, tuple(data.values()))
        self.connect.commit()


class BaseFilesPipeline(FilesPipeline):
    EXPIRES = 365 * 100

    def __init__(self, store_uri, download_func=None, settings=None):
        super().__init__(store_uri, download_func=download_func, settings=settings)
        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)
        self.ignore_404_error = settings.getbool("STD_IGNORE_FILES_DOWNLOAD_ON_404")

    def open_spider(self, spider):
        super().open_spider(spider)
        self.config = {}
        config_path = spider.settings.get("CONFIG_PATH")
        if os.path.exists(config_path):
            with open(config_path, "r") as _f:
                config = json.load(_f)
            for _cf in config:
                spname = spider.name
                if _cf.get(spname) and _cf.get(spname) != "0":
                    if isinstance(_cf[spname], (list, tuple)):
                        for post in _cf[spname]:
                            if post in self.config:
                                raise KeyError(post)
                            self.config[post] = _cf
                    elif isinstance(_cf[spname], (str, int)):
                        post = str(_cf[spname])
                        if post in self.config:
                            raise KeyError(post)
                        self.config[post] = _cf
                    else:
                        logger.warning(f"load config err: {_cf}")
        spider.config = self.config
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        for ok, value in results:
            if not ok:
                logger.error(
                    "%(class)s found errors processing",
                    {"class": self.__class__.__name__},
                    exc_info=failure_to_exc_info(value),
                    extra={"spider": info.spider},
                )
                raise DropItem("download fail")
        return item
    
    def media_downloaded(self, response, request, info, *, item=None):
        referer = referer_str(request)
        
        if response.status != 200:
            logger.warning(
                "File (code: %(status)s): Error downloading file from "
                "%(request)s referred in <%(referer)s>",
                {"status": response.status, "request": request, "referer": referer},
                extra={"spider": info.spider},
            )
            if response.status == 404 and self.ignore_404_error:
                return {
                    "url": request.url,
                    "path": "",
                    "checksum": "",
                    "status": "404",
                }
            raise FileException("download-error")

        if not response.body:
            logger.warning(
                "File (empty-content): Empty file from %(request)s referred "
                "in <%(referer)s>: no-content",
                {"request": request, "referer": referer},
                extra={"spider": info.spider},
            )
            raise FileException("empty-content")
        
        if 'Content-Length' in response.headers:
            if len(response.body) != int(response.headers['Content-Length']):
                logger.warning(
                    "File (code: %(status)s): Error downloading file from "
                    "%(request)s referred in <%(referer)s>",
                    {"status": response.status, "request": request, "referer": referer},
                    extra={"spider": info.spider},
                )
                raise FileException("download-content-not-enough")
        
        return super().media_downloaded(response, request, info, item=item)


class ProgressBarsPipeline:
    def __init__(self):
        manager = enlighten.get_manager()
        self.pbar = manager.counter(total=0, desc='Basic', unit='ticks')

    def open_spider(self, spider):
        spider.total_count = 0

    def process_item(self, item, spider):
        self.pbar.total = spider.total_count
        self.pbar.update()
        return item

    def close_spider(self, spider):
        pass
