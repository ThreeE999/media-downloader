import json
from pathlib import Path
import re
from urllib.parse import urlparse
import scrapy
import logging
from datetime import datetime
from setudownloader.pipelines import BaseFilesPipeline, ProgressBarsPipeline, SqlitePipeline
from setudownloader.middlewares import BaseDownloaderMiddleware
from setudownloader.define import NOTICE, GetLogFileName
import scrapy.signals
from scrapy.http.request import NO_CALLBACK
from setudownloader.spiders import BaseSpider
from bs4 import BeautifulSoup
from scrapy.pipelines.files import FileException

class NHItem(scrapy.Item):
    title = scrapy.Field()
    title_jpn = scrapy.Field()
    filecount = scrapy.Field()
    gid = scrapy.Field()

    url = scrapy.Field()
    page = scrapy.Field()


class NHDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        super().process_request(request, spider)
        request.headers['Referer'] = 'https://nhentai.net/'

    
    def process_response(self, request, response, spider: scrapy.Spider):
        if response.status in [429, 403]:
            spider.log(f"response.status = {response.status}", NOTICE)
            spider.crawler.engine.close_spider(spider, reason="request fail")
        elif response.status in [404]:
            spider.log(f"404Error url: <{request.url}>", logging.WARNING)
        return response
    

class NHDBPipeline(SqlitePipeline):
    db_path = ".database/nhentai.db"

    def open_spider(self, spider):
        super().open_spider(spider)
        self.build()
        # self.check()

    def build(self):
        # 修改数据库，要同时修改建库语句
        sql = """
            CREATE TABLE IF NOT EXISTS gallery
            (
                id          INT NOT NULL,
                title       TEXT,
                title_jpn   TEXT,
                count       INT,
                PRIMARY KEY (id)
            );
            CREATE TABLE IF NOT EXISTS media
            (   
                gallery_id  INT NOT NULL,
                page        INT NOT NULL,
                url         TEXT,
                PRIMARY KEY (gallery_id, page),
                FOREIGN KEY(gallery_id) REFERENCES gallery(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
        """
        self.cursor.executescript(sql)
    
    def process_item(self, item, spider):
        data = {
            "id": item["gid"],
            "title": item["title"],
            "title_jpn": item["title_jpn"],
            "count": item["filecount"],
        }
        self.insert("gallery", data)
        data = {
            "gallery_id": item["gid"],
            "page": item["page"],
            "url": item["url"][0],
        }
        self.insert("media", data)
        spider.log(f'{item["gid"]}-{item["page"]} database save', logging.INFO)
        return item


class NHFilesPipeline(BaseFilesPipeline):

    def get_media_requests(self, item, info):
        return [scrapy.Request(u, meta={"progress_bar_name":self.get_file_name(u, item)}) for u in item["url"]]

    def get_file_name(self, url, item):
        _path = Path(urlparse(url).path)
        return f"{item["page"]}-{_path.name}"

    def file_path(self, request, response=None, info=None, *, item=None):
        # 文件名处理
        _path = Path(urlparse(request.url).path)
        title = self.validate_and_normalize_filename(item['title_jpn'] or item["title"])
        user_path = "!other"
        media_path = f"{user_path}/nhentai/{title}/{_path.name}"
        return media_path
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        super().item_completed(results, item, info)
        if results:
            info.spider.log(f'{item["gid"]}-{item["page"]} download success: {[v.get("status") for k, v in results]}', NOTICE)
        return item


class NHProgressBarsPipeline(ProgressBarsPipeline):
    REQUEST_BAR_DEFAULT = False



class PixivSpider(BaseSpider):
    name = "nhentai"
    allowed_domains = ["nhentai.net"]
    start_urls = ["https://nhentai.net"]

    # 自定义setting
    custom_settings = {
        "MEDIA_ALLOW_REDIRECTS": True,  # 处理下载重定向

        "ITEM_PIPELINES": {
            "setudownloader.spiders.nhentai.NHFilesPipeline": 300,
            "setudownloader.spiders.nhentai.NHDBPipeline": 400,
            "setudownloader.spiders.nhentai.NHProgressBarsPipeline": 901,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.nhentai.NHDownloadMiddleware": 543,
        },
        "LOG_FILE": GetLogFileName(Path(__file__).stem),
        "LOG_LEVEL": "WARNING",
        "CONCURRENT_ITEMS": 1,  # 限制处理中的item数量
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2,
        "AUTOTHROTTLE_START_DELAY": 1,
    }

    def start_requests(self):
        """
        -a参数
        url=https://nhentai.net/g/499947/,...
        """
        # url = "https://nhentai.net/g/499947/"
        api_url = "https://nhentai.net/api/gallery/"
        pr = re.compile(r'nhentai.net/g/(\d+)')
        urls = getattr(self, "url", "").split(",")
        for url in urls:
            match = pr.search(url)
            if match:
                gid = match.group(1)
                cb_kwargs = {
                    "gid": gid,
                }
                _url = api_url + str(gid)
                yield scrapy.Request(url=_url, callback=self.parse, cb_kwargs=cb_kwargs)
            else:
                msg = f"不匹配的地址:{url}"
                print(msg)
                self.log(msg, logging.WARN)
    
    def parse(self, response, **kwargs):
        result = json.loads(response.text)
        
        gid = result["id"]
        media_id = result["media_id"]
        counts = result["num_pages"]
        ext_dict = {
            "j": "jpg",
            "p": "png",
        }
        self.add_total(counts)
        page = 0
        for p in result["images"]["pages"]:
            page += 1
            if self._check_pid_download(gid, page):
                self.log(f"跳过{gid}-{page}", logging.DEBUG)
                self.add_skip()
                continue
            if p["t"] not in ext_dict:
                self.log(f"类型不支持: {page}, {p}", logging.ERROR)
                return
            
            ext = ext_dict[p["t"]]
            item = NHItem()
            item["title"] = result["title"].get("english", "")
            item["title_jpn"] = result["title"].get("japanese", "")
            item["filecount"] = int(counts)
            item["gid"] = int(gid)
            item["url"] = [f"https://i7.nhentai.net/galleries/{media_id}/{page}.{ext}"]
            item["page"] = page
            yield item

    def _check_pid_download(self, gid, page):
        if self.force:
            return False
        sql = f"SELECT * FROM media WHERE gallery_id = {gid} and page = {page}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)
    
   

