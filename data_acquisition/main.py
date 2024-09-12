"""
Main file that runs the entire pipeline for data acquisition.
"""
import os

from spider_ri_ufrn import SpiderRIUFRN
from scrapy.crawler import CrawlerProcess
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    process = CrawlerProcess()

    process.crawl(SpiderRIUFRN, url_base=os.environ.get("URL_BASE"))
    process.start()
