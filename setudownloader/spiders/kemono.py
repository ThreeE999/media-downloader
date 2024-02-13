import json
import mimetypes
import os
from pathlib import Path
from urllib.parse import urlparse
import scrapy
import logging
from datetime import datetime
from scrapy.pipelines.files import FilesPipeline
from setudownloader.pipelines import SqlitePipeline
from scrapy.utils.log import failure_to_exc_info
from setudownloader.middlewares import BaseDownloaderMiddleware
from scrapy.exceptions import DropItem
from setudownloader.define import NOTICE

class KemonoItem(scrapy.Item):
    user = scrapy.Field()
    title = scrapy.Field()
    service = scrapy.Field()
    upload_data = scrapy.Field()
    id = scrapy.Field()
    file = scrapy.Field()
    content = scrapy.Field()

    file_urls = scrapy.Field()  # 下载链接


class KemonoDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        request.meta["proxy"] = self.proxy
        if self.cookies:
            request.cookies = self.cookies
        # request.headers['Referer'] = 'https://www.pixiv.net/discovery'
    
    def process_response(self, request, response, spider: scrapy.Spider):
        if response.status in [429, 403]:
            spider.log(f"response.status = {response.status}", NOTICE)
            spider.crawler.engine.close_spider(spider, reason="request fail")
        return response
    
class KemonoDBPipeline(SqlitePipeline):
    db_path = ".database/kemono.db"

    def open_spider(self, spider):
        super().open_spider(spider)
        self.build()
        # self.check()

    def build(self):
        # 修改数据库，要同时修改建库语句
        sql = """
            CREATE TABLE IF NOT EXISTS user
            (
                id          INT NOT NULL,
                name        TEXT NOT NULL,
                PRIMARY KEY (id)
            );

            CREATE TABLE IF NOT EXISTS illust
            (
                id          INT NOT NULL ,
                title       TEXT,
                user_id     INT,
                page_count  INT NOT NULL,
                upload_date DATETIME NOT NULL,
                type        INT NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY(user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

            CREATE TABLE IF NOT EXISTS media
            (
                illust_id   INT ,
                page        INT NOT NULL,
                url         TEXT NOT NULL,
                suffix      TEXT NOT NULL,
                is_download BOOLEAN NOT NULL,
                is_delete   BOOLEAN NOT NULL,
                PRIMARY KEY (illust_id, page),
                FOREIGN KEY(illust_id) REFERENCES illust(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
        """
        self.cursor.executescript(sql)
        update_sql = ""
        # update_sql = "ALTER TABLE illust ADD COLUMN title TEXT;"
        # 更新新字段时，可能会重新爬取大量元数据
        if update_sql:
            self.cursor.executescript(update_sql)
    
    def check(self):
        # 检查数据库没有下载的meida数据 TODO
        sql = "SELECT * FROM media WHERE is_download = 0;"
        obj = list(self.cursor.execute(sql))

    def process_item(self, item, spider):
        data = {
            "id": item["user_id"],
            "name": item["user_name"]
        }
        self.insert("user", data)

        data = {
            "id": item["illust_id"],
            "user_id": item["user_id"],
            "page_count": item["page_count"],
            "upload_date": item["upload_date"],
            "type": item["illust_type"],
            "title": item["illust_title"]
        }
        self.insert("illust", data)

        for i, url in enumerate(item["urls"]):
            data = {
                "illust_id": item["illust_id"],
                "page": i,
                "url": url,
                "suffix": Path(urlparse(url).path).suffix,
                "is_download": True,
                "is_delete": False
            }
            self.insert("media", data)
        spider.log(f'[{item["illust_id"]}] database save', NOTICE)
        return item



class KemonoFilesPipeline(FilesPipeline):
    EXPIRES = 365 * 100

    def open_spider(self, spider):
        super().open_spider(spider)
        self.config = {}
        config_path = spider.settings.get("CONFIG_PATH")
        if os.path.exists(config_path):
            with open(config_path, "r") as _f:
                config = json.load(_f)
            for _cf in config:
                if _cf.get(spider.name):
                    self.config[_cf.get(spider.name)] = _cf
        spider.config = self.config

    def process_item(self, item, spider):
        # item预处理
        item["file_urls"] = spider.get_need_download_urls(item)
        return super().process_item(item, spider)

    def file_path(self, request, response=None, info=None, *, item=None):
        # 文件名处理
        _path = Path(urlparse(request.url).path)
        media_ext = _path.suffix
        if media_ext not in mimetypes.types_map:
            media_ext = ""
            media_type = mimetypes.guess_type(request.url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        user_id = item['user_id']
        if int(user_id) in self.config:
            media_path = f"{self.config[int(user_id)]['path']}/pixiv/{user_id}/{_path.name}"
        elif str(user_id) in self.config:
            media_path = f"{self.config[str(user_id)]['path']}/pixiv/{user_id}/{_path.name}"
        else:
            media_path = f"other/pixiv/{user_id}/{_path.name}"
        return media_path
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        for ok, value in results:
            if not ok:
                info.spider.logger.error(
                    "%(class)s found errors processing",
                    {"class": self.__class__.__name__},
                    exc_info=failure_to_exc_info(value),
                    extra={"spider": info.spider},
                )
                raise DropItem("download fail")
        if results:
            info.spider.log(f'[{item["illust_id"]}] download success', NOTICE)
        return item


class KemonoSpider(scrapy.Spider):
    name = "kemono"
    allowed_domains = ["kemono.su"]
    start_urls = ["https://kemono.su"]

    # 自定义setting
    custom_settings = {
        # "ITEM_PIPELINES": {
        #     "setudownloader.spiders.pixiv.KemonoFilesPipeline": 300,
        #     "setudownloader.spiders.pixiv.KemonoDBPipeline": 400,
        # },
        # "DOWNLOADER_MIDDLEWARES": {
        #     "setudownloader.spiders.pixiv.KemonoDownloadMiddleware": 543,
        # },
        "LOG_LEVEL": "DEBUG",
    }

    def start_requests(self):
        # https://kemono.su/api/v1/fanbox/user/14496985
        user = 14496985
        service = "fanbox"
        cb_kwargs = {
            "user": str(user),
            "service": service,
        }
        url = f"https://kemono.su/api/v1/{service}/user/{user}"
        yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=cb_kwargs)

    def parse(self, response, **cb_kwargs):
        result = json.loads(response.text)
        for data in result:
            if data["file"] and data["file"] not in data["attachments"]:
                data["attachments"].insert(0, data["file"])
            kitem = KemonoItem()
            kitem["user"] = data["user"]
            kitem["title"] = data["title"]
            kitem["service"] = data["service"]
            kitem["upload_data"] = data["published"]
            kitem["id"] = data["id"]
            kitem["file"] = data["attachments"]
            kitem["content"] = data["content"]
            yield kitem
        if len(result) == 50:
            cb_kwargs["page"] = cb_kwargs.get("page", 0) + 50
            url = f"https://kemono.su/api/v1/{cb_kwargs['service']}/user/{cb_kwargs['user']}?o={cb_kwargs['page']}"
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=cb_kwargs)
    
    def _check_pid_download(self, pid):
        sql = f"SELECT * FROM illust WHERE id = {pid}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)
    
    
    



