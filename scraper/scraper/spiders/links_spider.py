"""Import scrapy - web scraping framework"""
from pathlib import Path
import scrapy
import tomli


class LinksSpider(scrapy.Spider):
    """Spider to crawl posts links from listing website."""
    name = 'links'
    base_url = ''
    start_urls = []

    # TODO: Write a function to get config

    def __init__(self):
        super().__init__()
        config_file_path = Path("scraper.toml").resolve()
        with open(config_file_path, mode="rb") as config_file:
            self.config = tomli.load(config_file)

        self.start_urls = self.config["links"]["start_urls"]
        self.base_url = self.config["links"]["base_url"]

    def parse(self, response):
        link_selector = self.config["selectors"]["links"]["link"]

        for post in response.css('.post'):
            yield {
                'link': self.base_url + post.css(link_selector).get()
            }

        next_page_selector = self.config["selectors"]["links"]["next_page"]
        next_page = response.css(next_page_selector).get()

        if next_page is not None:
            yield response.follow(next_page, self.parse)
