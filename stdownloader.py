from pathlib import Path
import scrapy
import sqlite3
import shutil
from setudownloader.settings import CONFIG_PATH
import os
import json
import sys
from scrapy.cmdline import execute

CONFIG_PATH_BAK = CONFIG_PATH + ".bak"

def _check_config_format():
    # 尝试解析 JSON 数据
    json_data = json.loads(CONFIG_PATH)
    # 确保是一个列表
    if not isinstance(json_data, list):
        raise ValueError("config格式错误")
    # 遍历列表中的每个字典
    for entry in json_data:
        if not isinstance(entry, dict):
            raise ValueError("config格式错误")


def _create_config_copy():
    shutil.copy2(CONFIG_PATH, CONFIG_PATH_BAK)
    os.chmod(CONFIG_PATH_BAK, 0o444)

def main(argv):
    # if os.path.exists(CONFIG_PATH):
    #     # config格式检查
    #     _check_config_format()
    #     # config变动检查-移动文件
    # _create_config_copy()

 
    # execute(argv=["scrapy", "crawl", "pixiv"])
    # execute(argv=["scrapy", "crawl", "twitter"])
    execute(argv=["scrapy", "crawl", argv[0]] + argv[1:])


if __name__ == "__main__":
    argv = sys.argv[1:]
    if argv:
        main(argv)
    # print(Path(__file__).stem)