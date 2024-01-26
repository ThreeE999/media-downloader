import hashlib
import json
import mimetypes
import os
from pathlib import Path
import scrapy
import logging
from datetime import date, datetime
from scrapy.utils.python import to_bytes
from itemadapter import ItemAdapter
from scrapy.pipelines.files import FilesPipeline
from setudownloader.pipelines import SqlitePipeline
from scrapy.http import Request
from scrapy.http.request import NO_CALLBACK

from setudownloader.middlewares import BaseDownloaderMiddleware

class PixivItem(scrapy.Item):
    user_name = scrapy.Field() # 作者名
    user_id = scrapy.Field()  # 作者 UID

    illust_id = scrapy.Field()  # 作品 PID
    illust_type = scrapy.Field()  # 作品类型 0插画 1漫画 2动图
    page_count = scrapy.Field()  # 作品页数
    upload_date = scrapy.Field()  # 上传日期

    urls = scrapy.Field()  # 所有图片原始链接

    file_urls = scrapy.Field()  # 下载链接


class PixivDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        if self.proxy and "pixiv.re" not in request.url:
            request.meta["proxy"] = self.proxy
        if self.cookies:
            request.cookies = self.cookies
        if 'pixiv.net' in request.url:
            request.headers['Referer'] = 'https://www.pixiv.net/discovery'

class PixivDBPipeline(SqlitePipeline):
    db_path = ".database/pixiv.db"

    def build(self):
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
        self.connect.commit()
    
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
            "type": item["illust_type"]
        }
        self.insert("illust", data)

        for i, url in enumerate(item["urls"]):
            data = {
                "illust_id": item["illust_id"],
                "page": i,
                "url": url,
                "suffix": Path(url).suffix,
                "is_download": True,
                "is_delete": False
            }
            self.insert("media", data)
        return item



class PixivFilesPipeline(FilesPipeline):

    def process_item(self, item, spider):
        self.config = {}
        config_path = spider.settings.get("CONFIG_PATH")
        if os.path.exists(config_path):
            with open(config_path, "r") as _f:
                config = json.load(_f)
            for _cf in config:
                if _cf.get(spider.name):
                    self.config[_cf.get(spider.name)] = _cf
        item["file_urls"] = [url.replace("pximg.net", "pixiv.re") for url in item["urls"]]
        return super().process_item(item, spider)

    def file_path(self, request, response=None, info=None, *, item=None):
        _path = Path(request.url)
        media_ext = _path.suffix
        # Handles empty and wild extensions by trying to guess the
        # mime type then extension or default to empty string otherwise
        if media_ext not in mimetypes.types_map:
            media_ext = ""
            media_type = mimetypes.guess_type(request.url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        media_path = f"downloaded/{_path.name}"
        return media_path


class PixivSpider(scrapy.Spider):
    name = "pixiv"
    allowed_domains = ["pixiv.net"]
    start_urls = ["https://pixiv.net"]

    # 自定义setting
    custom_settings = {
        "ITEM_PIPELINES": {
            "setudownloader.spiders.pixiv.PixivFilesPipeline": 300,
            "setudownloader.spiders.pixiv.PixivDBPipeline": 400,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.pixiv.PixivDownloadMiddleware": 543,
        }
    }

    def start_requests(self):
        # https://www.pixiv.net/ajax/user/41989573/profile/all
        uids = [41989573]        # 用户ID
        for uid in uids:
            url = f"https://www.pixiv.net/ajax/user/{uid}/profile/all"
            yield scrapy.Request(url=url, callback=self.profile_parse, dont_filter=True)

    def profile_parse(self, response):
        result = json.loads(response.text)
        if result["error"]:
            raise "author_parse请求失败 data:{result}"

        artworks = []
        illusts = list(result["body"]["illusts"])   # 所有插画
        manga = list(result["body"]["manga"])       # 所有漫画
        
        artworks.extend(illusts)
        artworks.extend(manga)

        self.log(f"作者作品数量为：{len(artworks)}", logging.INFO)

        for pid in artworks:
            url = f"https://www.pixiv.net/ajax/illust/{pid}"
            yield scrapy.Request(url=url, callback=self.illust_parse, dont_filter=True)
            return
            
    def illust_parse(self, response):
        result = json.loads(response.text)
        if result["error"]:
            raise "parse请求失败 data:{result}"
        result = result["body"]
        item = PixivItem()
        item["user_name"] = result["userName"]
        item["user_id"] = int(result["userId"])
        item["illust_id"] = illust_id = int(result["illustId"])
        item["illust_type"] = illust_type = int(result["illustType"])
        item["page_count"] = page_count = int(result["pageCount"])
        item["upload_date"] = datetime.strptime(result["uploadDate"], "%Y-%m-%dT%H:%M:%S%z")
        item["urls"] = []
        
        if illust_type == 2:    # 动图
            page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/ugoira_meta'
            yield scrapy.Request(page_url, callback=self.ugoira_parse, cb_kwargs={"item": item})
        else:
            if page_count == 1: # 只有一页就跳过, 减少请求次数:
                item['url'].append(result['urls']['original'])
                yield item
            else:
                page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/pages'
                yield scrapy.Request(page_url, callback=self.parse, cb_kwargs={"item": item})
    
    def parse(self, response, item):
        result = json.loads(response.text)
        for i in result['body']:
            item['urls'].append(i['urls']['original'])
        yield item

    def ugoira_parse(self, response, item):
        result = json.loads(response.text)
        item['urls'].append(result['body']["originalSrc"])
        yield item
