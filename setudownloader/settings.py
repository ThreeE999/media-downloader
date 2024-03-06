BOT_NAME = "setudownloader"

SPIDER_MODULES = ["setudownloader.spiders"]
NEWSPIDER_MODULE = "setudownloader.spiders"

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

# Obey robots.txt rules 不建议遵守
ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
#CONCURRENT_REQUESTS = 32

# 对同一域的 2 个连续请求之间等待的最短秒数
DOWNLOAD_DELAY = 0.6
# https://docs.scrapy.org/en/latest/topics/settings.html#std-setting-CONCURRENT_REQUESTS_PER_DOMAIN
CONCURRENT_REQUESTS_PER_DOMAIN = 16

COOKIES_ENABLED = True

# Disable Telnet Console (enabled by default)
TELNETCONSOLE_ENABLED = False

# Override the default request headers:
#DEFAULT_REQUEST_HEADERS = {
#    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
#    "Accept-Language": "en",
#}

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
   "setudownloader.middlewares.SetudownloaderSpiderMiddleware": 543,
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
   "setudownloader.middlewares.BaseDownloaderMiddleware": 543,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
#EXTENSIONS = {
#    "scrapy.extensions.telnet.TelnetConsole": None,
#}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#    "setudownloader.pipelines.SetudownloaderPipeline": 300,
# }

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
AUTOTHROTTLE_ENABLED = True
# The initial download delay
AUTOTHROTTLE_START_DELAY = 1
# The maximum download delay to be set in case of high latencies
AUTOTHROTTLE_MAX_DELAY = 20
# The average number of requests Scrapy should be sending in parallel to
# each remote server
#AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
#AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
#HTTPCACHE_ENABLED = True
#HTTPCACHE_EXPIRATION_SECS = 0
#HTTPCACHE_DIR = "httpcache"
#HTTPCACHE_IGNORE_HTTP_CODES = []
#HTTPCACHE_STORAGE = "scrapy.extensions.httpcache.FilesystemCacheStorage"

# Set settings whose default value is deprecated to a future-proof value
REQUEST_FINGERPRINTER_IMPLEMENTATION = "2.7"
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
FEED_EXPORT_ENCODING = "utf-8"

DOWNLOAD_MAXSIZE = 0
DOWNLOAD_TIMEOUT = 300
DOWNLOAD_WARNSIZE = 1024 * 1024 * 100  # 设置为你期望的字节数大小，例如 50 MB
CONCURRENT_ITEMS = 20

# CRITICAL, ERROR, WARNING, INFO, DEBUG
LOG_LEVEL = "WARNING"

# 文件下载根路径
FILES_STORE = "downloads"



# 代理
STD_HTTPPROXY = "http://127.0.0.1:10809"
# 指定 cookie 路径
STD_COOKIES_DIR = "./setudownloader/cookies"
# 作者信息，下载配置文件
CONFIG_PATH = "./setudownloader/config.json"

# 下载文件时404，忽略下载文件并存储至数据库, False每次404不会存储数据库，每次下载就会警告
STD_IGNORE_FILES_DOWNLOAD_ON_404 = True