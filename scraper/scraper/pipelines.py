import mysql.connector
import tomli


class ScraperPipeline:
    """Scrapy Item Pipeline: https://docs.scrapy.org/en/latest/topics/item-pipeline.html"""

    def __init__(self):
        with open("scraper.toml", mode="rb") as config_file:
            self.config = tomli.load(config_file)

        self.conn = mysql.connector.connect(
            host=self.config["database"]["host"],
            user=self.config["database"]["user"],
            password=self.config["database"]["password"],
            database=self.config["database"]["name"]
        )

        try:
            self.cur = self.conn.cursor()
            self.cur.execute("""
                       CREATE TABLE IF NOT EXISTS houses(
                           house_id INT NOT NULL AUTO_INCREMENT,
                           title VARCHAR(255),
                           description TEXT,
                           price INT,
                           tot_no_room INT,
                           area TEXT,
                           location TEXT,
                           pub_date VARCHAR(255),
                           link VARCHAR(768),
                           website TEXT,
                           created_at DATE,
                           updated_at DATE,
                           PRIMARY KEY (house_id),
                           UNIQUE INDEX unique_link USING HASH (link)
                       );
                       """)
        except mysql.connector.Error as e:
            print("Error while connecting to MySQL", e)
            exit(1)

    def process_item(self, item, spider):
        if spider.name == "links":
            self.cur.execute(
                """INSERT IGNORE INTO houses (link, website) VALUES (%s,%s)""",
                (item["link"], item["name"])
            )
            self.conn.commit()
        elif spider.name == "posts":
            # Check if the link already exists in the database
            self.cur.execute("SELECT EXISTS(SELECT 1 FROM houses WHERE link = %s)", (item["link"],))
            link_exists = self.cur.fetchone()[0]

            if link_exists:
                # If it exists, update only if the current item is more recent
                self.cur.execute(
                    """UPDATE houses
                    SET title = %s, description = %s, price = %s, tot_no_room = %s, area = %s, location = %s, pub_date = %s
                    WHERE link = %s
                    AND pub_date < %s  
                    """,
                    # Only update if the existing pub_date is older
                    (
                        item["title"],
                        item["description"],
                        item["price"],
                        item["tot_no_room"],
                        item["area"],
                        item["location"],
                        item["pub_date"],
                        item["link"],
                        item["pub_date"],  # Use the item's pub_date for comparison
                    )
                )
            else:
                # If it doesn't exist, insert a new record
                self.cur.execute(
                    """INSERT INTO houses (title, description, price, tot_no_room, area, location, pub_date, link, website)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                    (
                        item["title"],
                        item["description"],
                        item["price"],
                        item["tot_no_room"],
                        item["area"],
                        item["location"],
                        item["pub_date"],
                        item["link"],
                        item["name"]  # Assuming you want to store website for new records
                    )
                )

            self.conn.commit()
        else:
            raise ValueError("Invalid spider name!")

    def close_spider(self, spider):
        """Close cursor and database's connection.
        This method is called when the spider is closed."""
        self.cur.close()
        self.conn.close()
