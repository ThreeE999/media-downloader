# Define here the models for your spider middleware
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/spider-middleware.html

import os
from scrapy import signals
# useful for handling different item types with a single interface
from itemadapter import is_item, ItemAdapter
from http.cookiejar import MozillaCookieJar, Cookie
from requests.utils import dict_from_cookiejar

class SetudownloaderSpiderMiddleware:
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, or item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Request or item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


def list_from_cookiejar(cj):
    class _Cookie(Cookie):
        # 为兼容默认的cookies下载中间件不支持get方法的问题
        def get(self, name):
            return getattr(self, name)

        def __getitem__(self, name):
            return getattr(self, name)
        
    for cookie in cj:
        cookie.__class__ = _Cookie
    return cj


class BaseDownloaderMiddleware:
    # 做代理配置cookie中间件
    def __init__(self, settings) -> None:
        self.proxy = settings.get("STD_HTTPPROXY")
        cookies_file = settings.get("STD_COOKIES_FILE")
        cj = MozillaCookieJar()
        if cookies_file:
            cj.load(cookies_file)
        else:
            cookies_dir = settings.get("STD_COOKIES_DIR")
            if cookies_dir:
                for filename in os.listdir(cookies_dir):
                    cj.load(os.path.join(cookies_dir, filename))
        self.cookies = list_from_cookiejar(cj)

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(crawler.settings)
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        if self.proxy:
            request.meta["proxy"] = self.proxy
        if self.cookies:
            request.cookies = self.cookies
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info("Spider opened: %s" % spider.name)


