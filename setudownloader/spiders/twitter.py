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
from scrapy.utils.log import failure_to_exc_info
from setudownloader.middlewares import BaseDownloaderMiddleware
from scrapy.exceptions import DropItem
from urllib.parse import urlencode, urlparse

NOTICE = 35
logging.addLevelName(NOTICE, "NOTICE")

class TwitterItem(scrapy.Item):
    user_name = scrapy.Field() # 作者名
    user_screen_name = scrapy.Field()  # 作者 UID
    user_id = scrapy.Field()  # 作者 UID

    tweet_id = scrapy.Field()  # 作品 PID
    page_count = scrapy.Field()  # 作品页数
    upload_date = scrapy.Field()  # 上传日期

    urls = scrapy.Field()  # 所有图片原始链接

    file_urls = scrapy.Field()  # 下载链接


class TwitterDownloadMiddleware(BaseDownloaderMiddleware):
    def process_request(self, request, spider):
        request.headers['authorization'] = authorization
        for ck in self.cookies:
            if ck.name == "ct0":
                request.headers["x-csrf-token"] = ck.value
        request.meta["proxy"] = self.proxy
        if self.cookies:
            request.cookies = self.cookies
        request.headers['Referer'] = 'https://twitter.com/home'
    
    def process_response(self, request, response, spider: scrapy.Spider):
        return response

class TwitterDBPipeline(SqlitePipeline):
    db_path = ".database/twitter.db"

    def open_spider(self, spider):
        super().open_spider(spider)
        self.build()

    def build(self):
        # 修改数据库，要同时修改建库语句
        sql = """
            CREATE TABLE IF NOT EXISTS user
            (
                id          INT NOT NULL,
                screen_name TEXT NOT NULL,
                name        TEXT NOT NULL,
                PRIMARY KEY (id)
            );

            CREATE TABLE IF NOT EXISTS tweet
            (
                id          INT NOT NULL ,
                user_id     INT,
                page_count  INT NOT NULL,
                upload_date DATETIME NOT NULL,
                PRIMARY KEY (id),
                FOREIGN KEY(user_id) REFERENCES user(id) ON DELETE CASCADE ON UPDATE CASCADE
            );

            CREATE TABLE IF NOT EXISTS media
            (
                tweet_id    INT,
                page        INT NOT NULL,
                url         TEXT NOT NULL,
                suffix      TEXT NOT NULL,
                is_download BOOLEAN NOT NULL,
                is_delete   BOOLEAN NOT NULL,
                PRIMARY KEY (tweet_id, page),
                FOREIGN KEY(tweet_id) REFERENCES tweet(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
        """
        self.cursor.executescript(sql)

    def process_item(self, item, spider):
        data = {
            "id": item["user_id"],
            "name": item["user_name"],
            "screen_name": item["user_screen_name"]
        }
        self.insert("user", data)

        data = {
            "id": item["tweet_id"],
            "user_id": item["user_id"],
            "page_count": item["page_count"],
            "upload_date": item["upload_date"]
        }
        self.insert("tweet", data)

        for i, url in enumerate(item["urls"]):
            data = {
                "tweet_id": item["tweet_id"],
                "page": i,
                "url": url,
                "suffix": Path(urlparse(url).path).suffix,
                "is_download": True,
                "is_delete": False
            }
            self.insert("media", data)
        spider.log(f'[{item["tweet_id"]}] database save', NOTICE)
        return item



class TwitterFilesPipeline(FilesPipeline):
    EXPIRES = 365 * 100

    def open_spider(self, spider):
        super().open_spider(spider)
        self.config = {}
        config_path = spider.settings.get("CONFIG_PATH")
        if os.path.exists(config_path):
            with open(config_path, "r") as _f:
                config = json.load(_f)
            for _cf in config:
                if _cf.get(spider.name):
                    self.config[_cf.get(spider.name)] = _cf
        spider.config = self.config

    def process_item(self, item, spider):
        # item预处理
        item["file_urls"] = [f"{i}?name=orig" for i in item["urls"]]
        return super().process_item(item, spider)

    def file_path(self, request, response=None, info=None, *, item=None):
        # 文件名处理
        _path = Path(urlparse(request.url).path)
        media_ext = _path.suffix
        if media_ext not in mimetypes.types_map:
            media_ext = ""
            media_type = mimetypes.guess_type(request.url)[0]
            if media_type:
                media_ext = mimetypes.guess_extension(media_type)
        user_id = item['user_screen_name']
        tweet_id = item['tweet_id']
        page = item["file_urls"].index(request.url)
        if str(user_id) in self.config:
            media_path = f"{self.config[str(user_id)]['path']}/twitter/{user_id}/{tweet_id}_p{page}{media_ext}"
        else:
            media_path = f"other/twitter/{user_id}/{tweet_id}_p{page}{media_ext}"
        return media_path
    
    def item_completed(self, results, item, info):
        # 下载完成后，验证下载成功
        for ok, value in results:
            if not ok:
                info.spider.logger.error(
                    "%(class)s found errors processing",
                    {"class": self.__class__.__name__},
                    exc_info=failure_to_exc_info(value),
                    extra={"spider": info.spider},
                )
                raise DropItem("download fail")
        info.spider.log(f'[{item["tweet_id"]}] download success', NOTICE)
        return item

authorization = "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA"

userMediaApi = 'https://twitter.com/i/api/graphql/VJfe4DwUdfnVjjqS2rLhhQ/UserMedia'
userMediaApiParCommon = '{"responsive_web_graphql_exclude_directive_enabled":true,"verified_phone_label_enabled":false,"creator_subscriptions_tweet_preview_api_enabled":true,"responsive_web_graphql_timeline_navigation_enabled":true,"responsive_web_graphql_skip_user_profile_image_extensions_enabled":false,"c9s_tweet_anatomy_moderator_badge_enabled":true,"tweetypie_unmention_optimization_enabled":true,"responsive_web_edit_tweet_api_enabled":true,"graphql_is_translatable_rweb_tweet_is_translatable_enabled":true,"view_counts_everywhere_api_enabled":true,"longform_notetweets_consumption_enabled":true,"responsive_web_twitter_article_tweet_consumption_enabled":true,"tweet_awards_web_tipping_enabled":false,"freedom_of_speech_not_reach_fetch_enabled":true,"standardized_nudges_misinfo":true,"tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled":true,"rweb_video_timestamps_enabled":true,"longform_notetweets_rich_text_read_enabled":true,"longform_notetweets_inline_media_enabled":true,"responsive_web_media_download_video_enabled":false,"responsive_web_enhance_cards_enabled":false}'
userMediaApiPar = '{{"userId":"{}","count":{},{}"includePromotedContent":false,"withClientEventToken":false,"withBirdwatchNotes":false,"withVoice":true,"withV2Timeline":true}}'

userInfoApi = 'https://twitter.com/i/api/graphql/Vf8si2dfZ1zmah8ePYPjDQ/UserByScreenNameWithoutResults'
userInfoApiPar = '{{"screen_name":"{}","withHighlightedLabel":false}}'

hostUrl = 'https://api.twitter.com/1.1/guest/activate.json'


class TwitterSpider(scrapy.Spider):
    name = Path(__file__).stem
    allowed_domains = ["twitter.com"]
    start_urls = ["https://twitter.com"]

    # 自定义setting
    custom_settings = {
        "ITEM_PIPELINES": {
            "setudownloader.spiders.twitter.TwitterFilesPipeline": 300,
            "setudownloader.spiders.twitter.TwitterDBPipeline": 400,
        },
        "DOWNLOADER_MIDDLEWARES": {
            "setudownloader.spiders.twitter.TwitterDownloadMiddleware": 543,
        },
        "LOG_LEVEL": "WARNING",
    }

    def start_requests(self):
        uname = "teranekos"
        for uname in self.config:
            params = {'variables': userInfoApiPar.format(uname)}
            yield scrapy.FormRequest(url=userInfoApi, formdata=params, callback=self.user_parse)

    def user_parse(self, response):
        result = json.loads(response.text)
        cb = {
            "user_id" : result["data"]["user"]["rest_id"],
            "user_screen_name" : result["data"]["user"]["legacy"]["screen_name"],
            "media_count" : result["data"]["user"]["legacy"]["media_count"],
            "user_name" : result["data"]["user"]["legacy"]["name"],
        }
        self.log(f"{cb['user_screen_name']}")
        self.log(f"作者<{cb['user_screen_name']}>作品数量为：{cb['media_count']}", NOTICE)
        self.skipCount = {}
        return self.parse(**cb)


    def parse(self, response = None, **kwargs):
        if response:
            result = json.loads(response.text)
            instructions = result["data"]["user"]["result"]["timeline_v2"]["timeline"]["instructions"]
            cursorValue = None
            itemArray = []
            for instruction in instructions:
                for entry in instruction.get("entries", []):
                    if entry.get("content", {}).get("cursorType") == "Bottom":
                        cursorValue = entry["content"]["value"]
                    for item in entry.get("content", {}).get("items", []):
                        itemData = item["item"]["itemContent"]["tweet_results"]["result"]
                        itemArray.append(itemData)
                for item in instruction.get("moduleItems", []):
                    itemData = item["item"]["itemContent"]["tweet_results"]["result"]
                    itemArray.append(itemData)

            for itemData in itemArray:
                tweetItem = TwitterItem()
                if "tweet" in itemData:
                    itemData = itemData["tweet"]
                tweetItem["tweet_id"] = tweet_id = int(itemData["rest_id"])

                tweetItem["user_name"] = user_name = itemData["core"]["user_results"]["result"]["legacy"]["name"]
                tweetItem["user_screen_name"] = user_screen_name = itemData["core"]["user_results"]["result"]["legacy"]["screen_name"]
                tweetItem["user_id"] = user_id = int(itemData["core"]["user_results"]["result"]["rest_id"])
                
                tweetItem["page_count"] = page_count = len(itemData["legacy"]["extended_entities"]["media"])
                upload_date = itemData["legacy"]["created_at"]
                tweetItem["upload_date"] = datetime.strptime(upload_date, "%a %b %d %H:%M:%S %z %Y")
                tweetItem["urls"] = urls = []
                for media in itemData["legacy"]["extended_entities"]["media"]:
                    media_type = media["type"]
                    if media_type == "photo":
                        urls.append(media["media_url_https"])
                    elif media_type == "animated_gif":
                        url = media['video_info']['variants'][0]['url']
                        urls.append(url)
                    elif media_type == "video":
                        variants = sorted(media['video_info']['variants'],
                                          key=lambda s: s['bitrate'] if 'bitrate' in s else 0, reverse=True)[0]
                        url = variants['url']
                        urls.append(url)

                if str(user_screen_name) != kwargs["user_screen_name"] or str(user_id) != kwargs["user_id"]:
                    self.log(f"数据有误： {tweetItem}", logging.ERROR)
                    continue
                
                if self._check_pid_download(tweet_id):
                    self.log(f"跳过tid: {tweet_id}", logging.DEBUG)
                    # self.skipCount[kwargs["user_id"]] = self.skipCount.get(kwargs["user_id"], 0) + 1
                    continue
                yield tweetItem

            if cursorValue and itemArray and self.skipCount.get(kwargs["user_id"], 0) < 10:
                cursorPar = '"cursor":"{}",'.format(cursorValue)
                params = {
                    'variables': userMediaApiPar.format(kwargs["user_id"], 20, cursorPar),
                    'features': userMediaApiParCommon
                }
                url = userMediaApi + "?" + urlencode(params)
                # self.stop = True
                yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=kwargs)
        else:
            params = {
                'variables': userMediaApiPar.format(kwargs["user_id"], 20, ""),
                'features': userMediaApiParCommon
            }
            url = userMediaApi + "?" + urlencode(params)
            yield scrapy.Request(url=url, callback=self.parse, dont_filter=True, cb_kwargs=kwargs)

    def _check_pid_download(self, pid):
        sql = f"SELECT * FROM tweet WHERE id = {pid}"
        result = self.cursor.execute(sql).fetchall()
        if result:
            if None in result[0]:   # 某个字段为空，重新爬，因为理论上不会有字段为空
                return False
        return bool(result)

    
