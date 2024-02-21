import json
import logging
import os
import scrapy
from setudownloader.define import NOTICE

logging.addLevelName(NOTICE, "NOTICE")

class BaseSpider(scrapy.Spider):
    
    def __init__(self, name: str | None = None, **kwargs: json.Any):
        self._set_command_line_arguments()
        super().__init__(name, **kwargs)
        self._set_config()
    
    def _set_command_line_arguments(self):
        # -a 命令行参数补充
        self.sp_user = None   # 只处理单个作者

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