import json
import scrapy
import logging

from setudownloader.middlewares import SetudownloaderBaseDownloaderMiddleware



class PixivItem(scrapy.Item):
    user_name = scrapy.Field() # 作者
    user_id = scrapy.Field()  # 作者 UID
    illust_id = scrapy.Field()  # 作品 PID
    illust_type = scrapy.Field()  # 作品 PID
    page_count = scrapy.Field()  # 作品所在 P
    url = scrapy.Field()  # 图片链接


class DownloaderMiddleware(SetudownloaderBaseDownloaderMiddleware):
    pass

# 自定义setting
SETTINGS = {
    "DOWNLOADER_MIDDLEWARES": {
        "setudownloader.spiders.pixiv.DownloaderMiddleware": 543,
    },
}

class PixivSpider(scrapy.Spider):
    name = "pixiv"
    allowed_domains = ["pixiv.net"]
    start_urls = ["https://pixiv.net"]

    @classmethod
    def update_settings(cls, settings):     # 自定义setting
        super().update_settings(settings)
        settings_dict = {}
        for name, value in SETTINGS.items():
            if isinstance(settings.get(name), dict):
                _temp = settings.get(name)
                _temp.update(value)
                value = _temp
            settings_dict[name] = value
        settings.setdict(settings_dict, priority="spider")

    def start_requests(self):
        # https://www.pixiv.net/ajax/user/41989573/profile/all
        uids = [41989573]        # 用户ID
        for uid in uids:
            url = f"https://www.pixiv.net/ajax/user/{uid}/profile/all"
            yield scrapy.Request(url=url, callback=self.profile_parse, dont_filter=True)

    def profile_parse(self, response):
        result = json.loads(response.text)
        if result["error"]:
            raise "author_parse请求失败 data:{result}"

        artworks = []
        illusts = list(result["body"]["illusts"])   # 所有插画
        manga = list(result["body"]["manga"])       # 所有漫画
        
        artworks.extend(illusts)
        artworks.extend(manga)

        self.log(f"作者作品数量为：{len(artworks)}", logging.INFO)

        for pid in artworks:
            url = f"https://www.pixiv.net/ajax/illust/{pid}"
            yield scrapy.Request(url=url, callback=self.illust_parse, dont_filter=True)
            
    def illust_parse(self, response):
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
        item["url"] = []

        if illust_type == 2:    # 动图
            page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/ugoira_meta'
            yield scrapy.Request(page_url, callback=self.ugoira_parse, cb_kwargs={"item": item})
        else:
            if page_count == 1: # 只有一页就跳过, 减少请求次数:
                item['url'].append(result['urls']['original'])
                yield item
            else:
                page_url = f'https://www.pixiv.net/ajax/illust/{illust_id}/pages'
                yield scrapy.Request(page_url, callback=self.parse, cb_kwargs={"item": item})
    
    def parse(self, response, item):
        result = json.loads(response.text)
        for i in result['body']:
            item['url'].append(i['urls']['original'])
        yield item

    def ugoira_parse(self, response, item):
        result = json.loads(response.text)
        item['url'].append(result['body']["originalSrc"])
        yield item
