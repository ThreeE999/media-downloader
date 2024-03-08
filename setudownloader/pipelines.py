# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import json
import logging
import os
import re
import sqlite3
import enlighten
from itemadapter import ItemAdapter
import scrapy
from scrapy.pipelines.files import FilesPipeline, FileException
from scrapy.exceptions import DropItem
from scrapy.utils.log import failure_to_exc_info
from scrapy.utils.request import referer_str
from scrapy.settings import Settings
import scrapy.signals
import setudownloader.signals

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
        self.config = spider.config
    
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

    def validate_and_normalize_filename(self, filename):
        # Windows不允许的特殊字符
        illegal_chars_windows = r'[\\/:"*?<>|]'
        # Linux不建议使用的特殊字符
        illegal_chars_linux = r'[\/]'

        # 检查并替换特殊字符
        filename = re.sub(illegal_chars_windows, '_', filename)
        filename = re.sub(illegal_chars_linux, '_', filename)

        return filename

    def get_base_path(self, key):
        return self.config.get(key, {}).get("path", "!other")


class ProgressBarsPipeline:
    REQUEST_BAR_DEFAULT = "R"
    UNSHOW_SKIP_BAR = True
    
    def __init__(self):
        self.manager = manager = enlighten.get_manager()
        # self.pbar = self.manager.counter(total=0, desc='D', unit='p')
        self.request_bar = {}

        terminal = manager.term
        if self.UNSHOW_SKIP_BAR:
            bar_format = u'{desc}{desc_pad}{percentage:3.0f}%|{bar}| ' + \
                u'U:' + terminal.green3(u'{count_0:{len_total}}') + u' ' + \
                u'E:' + terminal.red2(u'{count_1:{len_total}}') + u' ' + \
                u'S:' + terminal.yellow2(u'{skip_count:{skip_count_len}}') + u' ' + \
                u'{count}/{total} ' + \
                u'[{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]'
            self.success = manager.counter(total=0, desc='D', unit='p', color='green3', bar_format=bar_format)
            self.success.fields["skip_count"] = 0
            self.success.fields["skip_count_len"] = 1
            self.failures = self.success.add_subcounter('red2')
        else:
            bar_format = u'{desc}{desc_pad}{percentage:3.0f}%|{bar}| ' + \
                u'U:' + terminal.green3(u'{count_0:{len_total}d}') + u' ' + \
                u'E:' + terminal.red2(u'{count_2:{len_total}d}') + u' ' + \
                u'S:' + terminal.yellow2(u'{count_1:{len_total}d}') + u' ' + \
                u'{count}/{total} ' + \
                u'[{elapsed}<{eta}, {rate:.2f}{unit_pad}{unit}/s]'

            self.success = manager.counter(total=-1, desc='D', unit='p', color='green3', bar_format=bar_format)
            self.skip = self.success.add_subcounter('yellow2')
            self.failures = self.success.add_subcounter('red2')

    def process_item(self, item, spider):
        self.success.update()
        # spider.crawler.signals.send_catch_log(
        #     signal=setudownloader.signals.change_success_count,
        # )
        return item

    def close_spider(self, spider):
        for request_id in self.request_bar:
            self.request_bar[request_id].close()
        self.request_bar.clear()
    
    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls()
        crawler.signals.connect(pipe.on_headers_received, signal=scrapy.signals.headers_received)
        crawler.signals.connect(pipe.on_bytes_received, signal=scrapy.signals.bytes_received)
        crawler.signals.connect(pipe.on_response_downloaded, signal=scrapy.signals.response_downloaded)
        crawler.signals.connect(pipe.on_change_total_count, signal=setudownloader.signals.change_total_count)
        crawler.signals.connect(pipe.on_change_skip_count, signal=setudownloader.signals.change_skip_count)
        # crawler.signals.connect(pipe.on_change_success_count, signal=setudownloader.signals.change_success_count)
        # crawler.signals.connect(pipe.on_change_fail_count, signal=setudownloader.signals.change_fail_count)

        crawler.signals.connect(pipe.on_change_fail_count, signal=scrapy.signals.item_dropped)
        crawler.signals.connect(pipe.on_change_fail_count, signal=scrapy.signals.item_error)
        return pipe

    def on_headers_received(self, headers, body_length, request, spider):
        bar_name = request.meta.get("progress_bar_name") or self.REQUEST_BAR_DEFAULT
        if bar_name:
            request_id = id(request)
            try:
                length = float(body_length)
            except:
                # 这里会有UNKNOWN_LENGTH的情况,直接不显示了
                length = float(0)
                spider.log(f"body_length: {body_length}, url: {request.url}", logging.DEBUG)
            BAR_FORMAT = '{desc}{desc_pad}{percentage:3.0f}%|{bar}| {count:!.2k}{unit} / {total:!.2k}{unit} ' \
                        '[{elapsed}<{eta}, {rate:!.2k}{unit}/s]'
            COUNTER_FMT = u'{desc}{desc_pad}{count} {unit}{unit_pad}' + \
              u'[{elapsed}, {rate:.2f}{unit_pad}{unit}/s]{fill}'
            self.request_bar[request_id] = self.manager.counter(total=length, desc=f' {bar_name}', unit='B', bar_format=BAR_FORMAT, leave=False, counter_format=COUNTER_FMT)
    
    def on_bytes_received(self, data, request, spider):
        request_id = id(request)
        if request_id in self.request_bar:
            self.request_bar[request_id].update(len(data))
    
    def on_response_downloaded(self, response, request, spider):
        request_id = id(request)
        if request_id in self.request_bar:
            self.request_bar[request_id].close()
            del self.request_bar[request_id]

    def on_change_total_count(self, count):
        self.success.total += count
    
    # def on_change_success_count(self, count):
    #     self.success.update()
    
    def on_change_skip_count(self, *args, **kwargs):
        if self.UNSHOW_SKIP_BAR:
            self.success.fields["skip_count"] += 1
            self.success.fields["skip_count_len"] = len(str(self.success.fields["skip_count"]))
            self.success.total -= 1
            self.success.refresh()
        else:
            self.skip.update()

    def on_change_fail_count(self, *args, **kwargs):
        self.failures.update()
