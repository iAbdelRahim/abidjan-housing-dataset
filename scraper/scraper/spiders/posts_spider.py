import re
import scrapy
import tomli
import mysql.connector


class PostsSpider(scrapy.Spider):
    """Spider to crawl post details from listing website."""
    name = 'posts'
    start_urls = []

    def __init__(self):
        super().__init__()
        with open("scraper.toml", mode="rb") as config_file:
            self.config = tomli.load(config_file)

        connection = mysql.connector.connect(
            host=self.config["database"]["host"],
            user=self.config["database"]["user"],
            password=self.config["database"]["password"],
            database=self.config["database"]["name"]
        )

        cursor = connection.cursor()
        cursor.execute("SELECT link FROM houses")
        links = cursor.fetchall()

        self.start_urls = [link[0] for link in links]

    def parse(self, response):
        title = self.config["selectors"]["posts"]["title"]
        price = self.config["selectors"]["posts"]["price"]
        location = self.config["selectors"]["posts"]["location"]
        published_at = self.config["selectors"]["posts"]["published_at"]
        description = self.config["selectors"]["posts"]["description"]
        tot_no_room = self.config["selectors"]["posts"]["tot_no_room"]
        area = self.config["selectors"]["posts"]["area"]

        yield {
            'title': self.remove_html(response.css(title).get()),
            'description': self.remove_html(response.css(description).get()),
            'price': float(self.remove_html(response.css(price).get())),
            'tot_no_room': self.get_tot_no_room(self.remove_html((response.css(tot_no_room).get()))),
            'area': self.get_area(self.remove_html(response.css(area).get())),
            'location': self.remove_html(response.css(location).get()),
            'published_at': self.remove_html(response.css(published_at).get()),
            'link': response.url,
        }

    def remove_html(self, string):
        """Remove HTML tags from a given string"""
        pattern = re.compile('<.*?>')
        try:
            return re.sub(pattern, ' ', string).strip()
        except Exception:  # Catch TypeError
            return None

    def get_tot_no_room(self, tot_no_room_string):
        """Parse and return the total number of rooms"""
        try:
            return int(tot_no_room_string)
        except Exception:  # Catch TypeError and ValueError
            return None

    def get_area(self, area_string):
        """Remove the unit (e.g.: m2) from the scraped area"""
        try:
            return float(re.search(r'\d+', area_string).group())
        except Exception:  # Catch TypeError and ValueError
            return None
