import json
import logging
import os
import scrapy
import setudownloader.signals
from setudownloader.define import NOTICE, NOTICE_WARN

logging.addLevelName(NOTICE, "NOTICE")
logging.addLevelName(NOTICE_WARN, "NOTICE WARN")

class BaseSpider(scrapy.Spider):
    
    def __init__(self, name = None, **kwargs):
        self._set_command_line_arguments()
        super().__init__(name, **kwargs)
        self._total = 0
        self._skip = 0
    
    def _set_command_line_arguments(self):
        # -a 命令行参数补充
        self.sp_user = None   # 只处理单个作者
    
    def add_total(self, add):   # 修改总数信号
        self._total += add
        self.crawler.signals.send_catch_log(
            signal=setudownloader.signals.change_total_count,
            count=add
        )
    
    def add_skip(self, add=1):   # 修改信号
        self._skip += add
        self.crawler.signals.send_catch_log(
            signal=setudownloader.signals.change_skip_count,
            count=self._skip
        )

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super().from_crawler(crawler, *args, **kwargs)
        spider._set_config()
        return spider
    
    @property
    def config(self):
        return self._config

    def _set_config(self) -> None:
        self._config = {}
        config_path = self.settings.get("CONFIG_PATH")
        if os.path.exists(config_path):
            with open(config_path, "r") as _f:
                config = json.load(_f)
            for _cf in config:
                spname = self.name
                if _cf.get(spname):
                    if isinstance(_cf[spname], (list, tuple)):
                        for post in _cf[spname]:
                            if isinstance(post, list):
                                post = tuple(list(map(str, post)))
                            if post in self._config:
                                raise KeyError(post)
                            self._config[post] = _cf
                    elif isinstance(_cf[spname], (str, int)):
                        post = str(_cf[spname])
                        if post in self._config:
                            raise KeyError(post)
                        self._config[post] = _cf
                    else:
                        self.log(f"load config err: {_cf}", logging.WARN)