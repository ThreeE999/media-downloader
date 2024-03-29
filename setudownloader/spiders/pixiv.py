import json
from pathlib import Path
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

class PixivItem(scrapy.Item):
    user_name = scrapy.Field() # 作者名
    user_id = scrapy.Field()  # 作者 UID

    illust_id = scrapy.Field()  # 作品 PID
    illust_title = scrapy.Field()
    illust_type = scrapy.Field()  # 作品类型 0插画 1漫画 2动图
    page_count = scrapy.Field()  # 作品页数
    upload_date = scrapy.Field()  # 上传日期

    urls = scrapy.Field()  # 所有图片原始链接


class PixivDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        if self.proxy and "pixiv.re" not in request.url:
            request.meta["proxy"] = self.proxy
        if self.cookies:
            request.cookies = self.cookies
        request.headers['Referer'] = 'https://www.pixiv.net/discovery'
    
    def process_response(self, request, response, spider: scrapy.Spider):
        if response.status in [429, 403]:
            spider.log(f"response.status = {response.status}", NOTICE)
            spider.crawler.engine.close_spider(spider, reason="request fail")
        elif response.status in [404]:
            spider.log(f"404Error request user: {request.cb_kwargs.get("user_id")}, url: <{request.url}>", logging.WARNING)
        return response
    

class PixivDBPipeline(SqlitePipeline):
    db_path = ".database/pixiv.db"

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
        spider.log(f'[{item["user_id"]}] {item["user_name"]} [{item["illust_id"]}] database save', NOTICE)
        return item


class PixivFilesPipeline(BaseFilesPipeline):

    def get_media_requests(self, item, info):
        return [scrapy.Request(u, callback=NO_CALLBACK, meta={"progress_bar_name":self.get_file_name(u)}) for u in item["urls"]]

    def get_file_name(self, url):
        _path = Path(urlparse(url).path)
        return _path.name

    def file_path(self, request, response=None, info=None, *, item=None):
        # 文件名处理
        _path = Path(urlparse(request.url).path)
        user_id = str(item['user_id'])
        user_path = self.get_base_path(user_id)
        media_path = f"{user_path}/pixiv/{user_id}/{_path.name}"
        return media_path
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        super().item_completed(results, item, info)
        if results:
            info.spider.log(f'[{item["illust_id"]}] download success', logging.INFO)
        return item



class PixivProgressBarsPipeline(ProgressBarsPipeline):
    REQUEST_BAR_DEFAULT = False



class PixivSpider(BaseSpider):
    name = "pixiv"
    allowed_domains = ["pixiv.net"]
    start_urls = ["https://pixiv.net"]

    # 自定义setting
    custom_settings = {
        "ITEM_PIPELINES": {
            "setudownloader.spiders.pixiv.PixivFilesPipeline": 300,
            "setudownloader.spiders.pixiv.PixivDBPipeline": 400,
            "setudownloader.spiders.pixiv.PixivProgressBarsPipeline": 901,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.pixiv.PixivDownloadMiddleware": 543,
        },
        "LOG_FILE": GetLogFileName(Path(__file__).stem),
        # "LOG_LEVEL": "DEBUG",
    }

    def start_requests(self):
        # https://www.pixiv.net/ajax/user/41989573/profile/all
        if self.sp_id:      # 按作品ID的模式
            artworks = self.sp_id.split(",")
            self.add_total(len(artworks))
            for pid in artworks:
                if self._check_pid_download(pid):
                    self.log(f"跳过pid: {pid}", logging.DEBUG)
                    self.add_skip()
                    continue
                else:
                    url = f"https://www.pixiv.net/ajax/illust/{pid}"
                    yield scrapy.Request(url=url, callback=self.illust_parse, dont_filter=True)
        else:           # 按作者ID的模式
            uids = list(self.config.keys())
            if self.sp_user:
                uids = self.sp_user.split(",")
            for uid in uids:
                url = f"https://www.pixiv.net/ajax/user/{uid}/profile/all"
                yield scrapy.Request(url=url, callback=self.profile_parse, dont_filter=True, cb_kwargs={"user_id": str(uid)})

    def profile_parse(self, response, **cb_kwargs):
        result = json.loads(response.text)
        if result["error"]:
            raise "author_parse请求失败 data:{result}"

        artworks = []
        illusts = list(result["body"]["illusts"])   # 所有插画
        manga = list(result["body"]["manga"])       # 所有漫画
        
        artworks.extend(illusts)
        artworks.extend(manga)
        user_id = cb_kwargs.get("user_id")
        user_name = self.config.get(user_id, {}).get("path", "no name")
        self.log(f"[{user_id}] {user_name} 作品数量为：{len(artworks)}", NOTICE)
        self.add_total(len(artworks))

        for pid in artworks:
            if self._check_pid_download(pid):
                self.log(f"跳过pid: {pid}", logging.DEBUG)
                self.add_skip()
                continue
            else:
                url = f"https://www.pixiv.net/ajax/illust/{pid}"
                yield scrapy.Request(url=url, callback=self.illust_parse, dont_filter=True)
    
    def _check_pid_download(self, pid):
        if self.force:
            return False
        sql = f"SELECT * FROM illust WHERE id = {pid}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)
    

    def illust_parse(self, response):
        # ex: https://www.pixiv.net/ajax/illust/82775556
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
        item["illust_title"] = result["title"]
        item["urls"] = []
        
        if illust_type == 2:    # 动图
            page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/ugoira_meta'
            return scrapy.Request(page_url, callback=self.ugoira_parse, cb_kwargs={"item": item})
        else:
            if page_count == 1: # 只有一页就跳过, 减少请求次数:
                item['urls'].append(result['urls']['original'])
                return item
            else:
                page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/pages'
                return scrapy.Request(page_url, callback=self.parse, cb_kwargs={"item": item})
    
    def parse(self, response, item):
        result = json.loads(response.text)
        for i in result['body']:
            item['urls'].append(i['urls']['original'])
        return item

    def ugoira_parse(self, response, item):
        result = json.loads(response.text)
        item['urls'].append(result['body']["originalSrc"])
        return item
