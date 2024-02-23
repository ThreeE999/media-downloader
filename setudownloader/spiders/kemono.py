from io import BytesIO
import json
import scrapy
import logging
from datetime import datetime
from setudownloader.pipelines import ProgressBarsPipeline, SqlitePipeline, BaseFilesPipeline
from setudownloader.middlewares import BaseDownloaderMiddleware
from scrapy.exceptions import DropItem
from setudownloader.define import NOTICE, GetLogFileName
from setudownloader.spiders import BaseSpider
from scrapy.http.request import NO_CALLBACK

class KemonoItem(scrapy.Item):
    user_id = scrapy.Field()
    title = scrapy.Field()
    service = scrapy.Field()
    upload_date = scrapy.Field()
    id = scrapy.Field()
    file = scrapy.Field()
    content = scrapy.Field()


class KemonoDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        super().process_request(request, spider)
        request.headers['Referer'] = 'https://kemono.su'
    
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

    def build(self):
        # 修改数据库，要同时修改建库语句
        sql = """
            CREATE TABLE IF NOT EXISTS fanbox
            (
                id          INT NOT NULL ,
                title       TEXT,
                user_id     INT NOT NULL,
                upload_date DATETIME NOT NULL,
                PRIMARY KEY (id)
            );
        """
        self.cursor.executescript(sql)


    def process_item(self, item, spider):
        if item["service"] == "fanbox":
            data = {
                "id": item["id"],
                "user_id": item["user_id"],
                "upload_date": item["upload_date"],
                "title": item["title"]
            }
            self.insert(item["service"], data)
        else:
            return DropItem("不支持的service！")
        spider.log(f'[{item["service"]}] [{item["user_id"]}] [{item["id"]}] database save', NOTICE)
        return item



class KemonoFilesPipeline(BaseFilesPipeline):

    def get_media_requests(self, item, info):
        return [
            scrapy.Request(
                f"https://kemono.su/data{u['path']}", 
                callback=NO_CALLBACK, 
                meta={"progress_bar_name": f"R{i+1}"}
            ) for i, u in enumerate(item["file"])
        ]

    def _path_by_item(self, item, name):
        user_id = item['user_id']
        service = item["service"]
        date = item["upload_date"]
        id = item["id"]
        title = item["title"]
        key = (service, str(user_id))
        if key in self.config:
            auther = self.config[key]["path"]
        else:
            auther = "other"
        title = self.validate_and_normalize_filename(title)
        media_path = f"{auther}/kemono/{service}/{user_id}/[{date.strftime("%Y%m%d")}] [{id}] {title}/{name}"
        return media_path

    def file_path(self, request, response=None, info=None, *, item=None):
        # 文件名处理
        for idx, i in enumerate(item["file"]):
            if i["path"] in request.url:
                name = i["name"]
                break
        name = f'{idx+1}_{name}'
        return self._path_by_item(item, name)
    
    def content_path(self, item):
        return self._path_by_item(item, "content.txt")

    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        super().item_completed(results, item, info)
        content = item["content"].encode('utf-8')
        if content:
            path = self.content_path(item)
            buf = BytesIO(content)
            buf.seek(0)
            self.store.persist_file(path, buf, info)
        if results:
            info.spider.log(f'[{item["service"]}][{item["id"]}] download success', logging.INFO)
        return item


class KemonoProgressBarsPipeline(ProgressBarsPipeline):
    REQUEST_BAR_DEFAULT = False


class KemonoSpider(BaseSpider):
    name = "kemono"
    allowed_domains = ["kemono.su"]
    start_urls = ["https://kemono.su"]

    # 自定义setting
    custom_settings = {
        "MEDIA_ALLOW_REDIRECTS": True,  # 处理下载重定向

        "ITEM_PIPELINES": {
            "setudownloader.spiders.kemono.KemonoFilesPipeline": 300,
            "setudownloader.spiders.kemono.KemonoDBPipeline": 400,
            "setudownloader.spiders.kemono.KemonoProgressBarsPipeline": 901,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.kemono.KemonoDownloadMiddleware": 543,
        },
        "LOG_LEVEL": "WARNING",
        "LOG_FILE": GetLogFileName("kemono"),
        "DOWNLOAD_WARNSIZE": 1024 * 1024 * 1024 * 1,
        "CONCURRENT_ITEMS": 1,  # 限制处理中的item数量
    }

    def start_requests(self):
        # https://kemono.su/api/v1/fanbox/user/14496985
        if getattr(self, "sp_user", None):
            ulst = [self.sp_user.split(",")]
        else:
            ulst = self.config.keys()    
        for service, user in ulst:
            cb_kwargs = {
                "user": str(user),
                "service": service,
            }
            url = f"https://kemono.su/api/v1/{service}/user/{user}"
            self.log(f"[{user}] [{service}] scaner", NOTICE)
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=cb_kwargs)

    def parse(self, response, **cb_kwargs):
        result = json.loads(response.text)
        self.add_total(len(result))
        for data in result:
            if data["file"] and data["file"] not in data["attachments"]:
                data["attachments"].insert(0, data["file"])
            kitem = KemonoItem()
            kitem["user_id"] = data["user"]    # 作者ID
            kitem["title"] = data["title"]  # 标题
            kitem["service"] = service = data["service"]          # 服务
            kitem["upload_date"] = datetime.fromisoformat(data["published"])    # 上传日期
            kitem["id"] = pid = data["id"]                # 作品ID
            kitem["file"] = data["attachments"]         # 附件
            kitem["content"] = data["content"]          # 元内容
            try:
                int(kitem["id"])
            except:
                self.log(f"错误数据跳过 {kitem}", logging.INFO)
                self.add_total(-1)
                continue
            if self._check_download(kitem["service"], kitem["id"]):
                self.log(f"跳过 {service}-{pid}", logging.INFO)
                self.add_skip()
                continue
            yield kitem
             
        if len(result) == 50:
            cb_kwargs["page"] = cb_kwargs.get("page", 0) + 50
            url = f"https://kemono.su/api/v1/{cb_kwargs['service']}/user/{cb_kwargs['user']}?o={cb_kwargs['page']}"
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=cb_kwargs)
    
    def _check_download(self, service, pid):
        sql = f"SELECT * FROM {service} WHERE id = {pid}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)
    



