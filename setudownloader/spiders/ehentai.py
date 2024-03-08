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
from scrapy.http import TextResponse
from scrapy.pipelines.files import FileException

class EHItem(scrapy.Item):
    title = scrapy.Field()
    title_jpn = scrapy.Field()
    filecount = scrapy.Field()
    gallery_token = scrapy.Field()
    media_token = scrapy.Field()
    gid = scrapy.Field()

    url = scrapy.Field()
    page = scrapy.Field()


class EHDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        super().process_request(request, spider)
        request.headers['Referer'] = 'https://exhentai.net/'

    
    def process_response(self, request, response, spider: scrapy.Spider):
        if response.status in [429, 403]:
            spider.log(f"response.status = {response.status}", NOTICE)
            spider.crawler.engine.close_spider(spider, reason="request fail")
        elif response.status in [404]:
            spider.log(f"404Error url: <{request.url}>", logging.WARNING)
        return response
    

class EHDBPipeline(SqlitePipeline):
    db_path = ".database/ehentai.db"

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
                token       TEXT,
                count       INT,
                PRIMARY KEY (id)
            );
            CREATE TABLE IF NOT EXISTS media
            (   
                gallery_id  INT NOT NULL,
                page        INT NOT NULL,
                url         TEXT,
                token       TEXT,
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
            "token": item["gallery_token"],
            "count": item["filecount"],
        }
        self.insert("gallery", data)
        data = {
            "gallery_id": item["gid"],
            "page": item["page"],
            "url": item["url"][0],
            "token": item["media_token"],
        }
        self.insert("media", data)
        spider.log(f'{item["gid"]}-{item["page"]} database save', logging.INFO)
        return item


class EHFilesPipeline(BaseFilesPipeline):

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
        media_path = f"{user_path}/ehentai/{title}/{_path.name}"
        return media_path
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        super().item_completed(results, item, info)
        if results:
            info.spider.log(f'{item["gid"]}-{item["page"]} download success: {[v.get("status") for k, v in results]}', NOTICE)
        return item

    def media_downloaded(self, response, request, info, *, item=None):
        if isinstance(response, TextResponse):
            if "you do not have enough" in response.text:
                info.spider.crawler.engine.close_spider(info.spider, reason=str(response.text))
                raise FileException(response.text)
            elif "Invalid token" in response.text:
                raise FileException(response.text)
        return super().media_downloaded(response, request, info, item=item)

class EHProgressBarsPipeline(ProgressBarsPipeline):
    REQUEST_BAR_DEFAULT = False



class PixivSpider(BaseSpider):
    name = "ehentai"
    allowed_domains = ["e-hentai.net", "exhentai.net"]
    start_urls = ["https://e-hentai.net"]

    # 自定义setting
    custom_settings = {
        "MEDIA_ALLOW_REDIRECTS": True,  # 处理下载重定向

        "ITEM_PIPELINES": {
            "setudownloader.spiders.ehentai.EHFilesPipeline": 300,
            "setudownloader.spiders.ehentai.EHDBPipeline": 400,
            "setudownloader.spiders.ehentai.EHProgressBarsPipeline": 901,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.ehentai.EHDownloadMiddleware": 543,
        },
        "LOG_FILE": GetLogFileName(Path(__file__).stem),
        "LOG_LEVEL": "WARNING",
        "CONCURRENT_ITEMS": 1,  # 限制处理中的item数量
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 3,
        "AUTOTHROTTLE_START_DELAY": 2,
    }

    def start_requests(self):
        """
        -a参数
        url=https://exhentai.org/g/2569702/a6e2be081b/,...
        fix=1   一直下载错误时尝试修复（可能是代理的问题）
        """
        # url = "https://exhentai.org/g/2569702/a6e2be081b/"
        pr = re.compile(r'(e[x-]hentai).org/g/(\d+)/(\w+)')
        urls = getattr(self, "url", "").split(",")
        for url in urls:
            match = pr.search(url)
            if match:
                host = match.group(1)
                gid = match.group(2)
                token = match.group(3)
                cb_kwargs = {
                    "url": url,
                    "host": host,
                    "gid": gid,
                    "token": token,
                }
                params = {
                    "method": "gdata",
                    "gidlist": [
                        [int(gid), token]
                    ],
                    "namespace": 1
                }
                url = f"https://api.e-hentai.org/api.php"
                yield scrapy.Request(url=url, method="POST", body=json.dumps(params), callback=self.g_parse, cb_kwargs=cb_kwargs)
            else:
                msg = f"不匹配的地址:{url}"
                print(msg)
                self.log(msg, logging.WARN)
    
    def g_parse(self, response, **kwargs):
        result = json.loads(response.text)
        if "gmetadata" not in result:
            self.log("gid或token有误", logging.ERROR)
            return
        
        kwargs.update(result["gmetadata"][0])
        kwargs["g_token"] = kwargs.get("token")
        url = f"https://{kwargs['host']}.org/g/{kwargs['gid']}/{kwargs['g_token']}/"
        filecount = int(kwargs["filecount"])
        gid = int(kwargs["gid"])

        need_scan_gpage2ipage = {}
        self.log(f"URL:{url}, page:{filecount}", NOTICE)
        self.add_total(filecount)
        for i in range(1, filecount+1):
            if self._check_pid_download(gid, i):
                self.log(f"跳过{gid}-{i}", logging.DEBUG)
                self.add_skip()
                continue
            else:
                gpage = int((i-1) / 40)
                need_scan_gpage2ipage.setdefault(gpage, []).append(i)
        
        need_scan_gpage2ipage = sorted([(k,v) for k,v in need_scan_gpage2ipage.items()], key=lambda x: x[0], reverse=True)
        for gpage, ipages in need_scan_gpage2ipage:
            kwargs["pages"] = ipages.copy()
            _URL = f"{url}?p={gpage}"
            yield scrapy.Request(url=_URL, callback=self.parse, dont_filter=True, cb_kwargs=kwargs)

    def _check_pid_download(self, gid, page):
        if self.force:
            return False
        sql = f"SELECT * FROM media WHERE gallery_id = {gid} and page = {page}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)
    
    def parse(self, response, **kwargs):
        bs = BeautifulSoup(response.text, features="lxml")
        cur_page = bs.find_all(class_="gdtm")
        pages = kwargs["pages"].copy()
        for page in pages[::-1]:
            url = cur_page[page%40-1].find("a")["href"]
            kwargs["page"] = page
            # https://exhentai.org/s/2f6cc5669b/2849352-3
            pr = re.compile(r'e[x-]hentai.org/s/(\w+)/(\d+)-(\d+)')
            match = pr.search(url)
            if match:
                kwargs["m_token"] = match.group(1)
            yield scrapy.Request(url=url, callback=self.sparse, dont_filter=True, cb_kwargs=kwargs)

    def sparse(self, response, **kwargs):
        bs = BeautifulSoup(response.text, features="lxml")

        urls = []
        url = _url = bs.find(id="i3").find(id="img")["src"]
        for _a in bs.find(id="i6").find_all("a"):
            if "original" in _a.text:
                url = _a["href"]
                urls.append(_a["href"])
                break
        if getattr(self, "fix", False):
            urls.append(_url)
        # next_url = bs.find(id="i4").find(id="next")

        item = EHItem()
        item["title"] = kwargs.get("title", "")
        item["title_jpn"] = kwargs.get("title_jpn", "")
        item["filecount"] = int(kwargs.get("filecount", 0))
        item["gallery_token"] = kwargs.get("g_token", "")
        item["media_token"] = kwargs.get("m_token", "")
        item["gid"] = int(kwargs.get("gid"))
        item["url"] = urls
        item["page"] = int(kwargs.get("page"))
        yield item
