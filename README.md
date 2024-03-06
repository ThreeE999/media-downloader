# 媒体下载器

## 介绍

- 此脚本的目的是为了方便自己定时下载各个平台的图片文件，因为有些图片是需要删除的并避免下载，但是目前网上的爬虫一般不支持去重。

- 针对作者的媒体爬虫，基于scrapy实现。

- 将每个作者的不同平台下的媒体文件存放在同一个文件夹分类整理，支持单个平台多账户的情况。

- 使用sqlite存放已经下载过的媒体文件，避免重复下载。在不修改数据库的情况下，当文件被手动删除或移动也不会重新下载。

- 目前支持pixiv、twitter、kemono。

## 安装

- 开发环境python3.12.1，推荐conda
```
pip install -r ./requirements.txt
```

## 使用

- 配置自己的config.json文件, 文件格式：
```
[
    {
        "path": "文件路径",
        "pixiv": "作者ID",
        "twitter": "用户名",
        "kemono": [
            ["服务", "作者ID"],
            ["fanbox", "作者ID"],
        ]
    },
    {
        "path": "文件路径",
        "pixiv": "作者ID",
        "twitter": "用户名",
        "kemono": [
            ["服务", "作者ID"],
            ["fanbox", "作者ID"],
        ]
    }
]
```
- 在setting.py中需要更改的几个设置
```
# 文件下载根路径
FILES_STORE = "downloads"
# 代理
STD_HTTPPROXY = "http://127.0.0.1:10809"
# 指定 cookie 路径, （没cookie可下载不了r18
STD_COOKIES_DIR = "./setudownloader/cookies"
# 下载配置文件
CONFIG_PATH = "./setudownloader/config.json"
```

- 运行
```
# 全配置pixiv作者爬虫
python stdownloader.py pixiv

# 单个twitter用户
python stdownloader.py twitter -a sp_user=setuauther

# 单个fanbox用户
python stdownloader.py kemono -a sp_user=fanbox,123456
```

## 拓展

新增网站的话新建spider就行，目前功能反正是能跑，异常能处理的就处理了。目前是自己边用边改，会定期上传更新代码。

还有可以优化的地方，听我说你先别急。