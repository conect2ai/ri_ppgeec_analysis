"""
File that contains the spider to scrape the data from the RI-UFRN repository.
"""
import logging.config
import re
import csv
import scrapy
import logging
import pandas as pd
import numpy as np

from utils import clean_text, convert_date
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime


now = datetime.now()
dt_string = now.strftime("%Y_%m_%d_%H_%M_%S")

logging.basicConfig(
    filename=f"./logs/spider_ri_ufrn_{dt_string}.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)


class SpiderRIUFRN(scrapy.Spider):
    name: str = "scraper_ri_ufrn"

    def __init__(self, url_base=None, *args, **kwargs):
        super(SpiderRIUFRN, self).__init__(*args, **kwargs)

        if not url_base:
            raise ValueError("To initialize the spider, you need to pass the base URL using the parameter 'url_base'.")
        
        self.url_base = url_base
        self.data = list()
        logging.info(f"Spider initialized with base URL: {self.url_base}")

    def start_requests(self):
        logging.info("Starting to request initial URL...")
        yield scrapy.Request(
            url=self.url_base,
            callback=self.parse_categories
        )

    def parse_categories(self, response):
        logging.info("Parsing categories...")
        css = "#content > div:nth-child(3) > div > div.col-md-9 > div.container.row > div > div.list-group"
        soup = BeautifulSoup(response.text, "html.parser")
        soup = soup.select(css).pop()
        urls = [(item.string.strip(), item["href"]) for item in soup.select("a")]
        logging.info(f"Found {len(urls)} categories.")
        
        for url in urls:
            logging.info(f"Requesting category: {url[0]}")
            yield response.follow(
                url=url[1],
                callback=self.parse_thesis_links,
                meta={"category": url[0]}
            )

    def parse_thesis_links(self, response):
        category = response.meta["category"] if "category" in response.meta else None
        links = response.meta["links"] if "links" in response.meta else dict()

        if category not in links:
            links[category] = list()

        logging.info(f"Parsing thesis links for category: {category}")
        css = "#content > div:nth-child(3) > div > div.col-md-9 > table"
        soup = BeautifulSoup(response.text, "html.parser")
        urls = soup.select(css).pop()
        urls = [item["href"] for item in urls.find_all("a") if item.has_attr("href")]
        logging.info(f"Found {len(urls)} thesis links in category: {category}")
        
        if len(urls) > 0:
            links[category].extend(urls)

        css = "#content > div:nth-child(3) > div > div.col-md-9 > div:nth-child(7)"
        soup = soup.select(css).pop()
        url = soup.find_all("a", string=re.compile(r"(next|próximo)", flags=re.IGNORECASE))

        if len(url) > 0:
            url = url[0]["href"]

            logging.info(f"Found next page, requesting: {url}")
            yield response.follow(
                url=url,
                callback=self.parse_thesis_links,
                meta={"category": category, "links": links}
            )
        else:
            logging.info(f"All thesis links for category {category} parsed. Moving to data extraction.")
            for url in links[category]:
                yield response.follow(
                    url=url,
                    callback=self.parse_data,
                    meta={"category": category}
                )

    def parse_data(self, response):
        category = response.meta["category"] if "category" in response.meta else None
        soup = BeautifulSoup(response.text, "html.parser")
        css = "table.table.itemDisplayTable"
        html = soup.select(css).pop()
        record = {"category": category}

        logging.info(f"Parsing data for thesis in category: {category}")
        try:
            for tag in html.select("table > tr")[:-1]:
                label = tag.select_one("tr > td.metadataFieldLabel")
                label = label.text.strip().lower().replace(":", "")
                record[label] = tag.select_one("tr > td.metadataFieldValue")
                record[label] = record[label].text.strip().replace("Resumo", "") if record[label] is not None else None

            css = "div.panel.panel-info > table > tr:nth-child(2) > td:first-child > a"
            record["document_url"] = soup.select_one(css)["href"]
            record["document_url"] = urljoin(self.url_base, record["document_url"])
        except Exception as e:
            logging.error(f"Error in extracting data for category {category} at URL {response.url}: {e}")

        self.data.append(record)
        logging.info(f"Data successfully parsed and added for category: {category}")

    def closed(self, reason):
        logging.info(f"Spider closed due to: {reason}. Processing collected data...")

        df = pd.DataFrame(
            self.data
        )

        df.replace(
            {np.nan: None},
            inplace=True
        )

        df.rename(
            columns={
                "keywords": "auth_keywords",
                "issue date": "defense_date",
                "portuguese abstract": "pt_abstract",
                "abstract": "en_abstract"
            },
            inplace=True
        )

        df.drop(
            columns=[
                "other titles",
                "embargoed until"
            ],
            inplace=True
        )

        logging.info("Columns normalized and unnecessary columns dropped.")

        df["category"] = df["category"].apply(lambda x: "PhD" if "Doutorado" in x else "MSc" if "Mestrado" in x else "Other")

        df.loc[df["authors"].notnull(), "authors"] = df.loc[df["authors"].notnull(), "authors"].apply(
            lambda x: f"{x.split(',')[1].strip()} {x.split(',')[0].strip()}"
        )
        df.loc[df["advisor"].notnull(), "advisor"] = df.loc[df["advisor"].notnull(), "advisor"].apply(
            lambda x: f"{x.split(',')[1].strip()} {x.split(',')[0].strip()}"
        )
        df.loc[df["auth_keywords"].notnull(), "auth_keywords"] = df.loc[
            df["auth_keywords"].notnull(), "auth_keywords"
        ].apply(
            lambda x: tuple([clean_text(k).strip() for k in x.split(";") if len(clean_text(k).strip())])
        )

        df.loc[df["title"].notnull(), "title"] = df.loc[df["title"].notnull(), "title"].apply(clean_text)
        df.loc[df["citation"].notnull(), "citation"] = df.loc[df["citation"].notnull(), "citation"].apply(clean_text)
        df.loc[df["pt_abstract"].notnull(), "pt_abstract"] = df.loc[ df["pt_abstract"].notnull(), "pt_abstract"].apply(clean_text)
        df.loc[df["en_abstract"].notnull(), "en_abstract"] = df.loc[df["en_abstract"].notnull(), "en_abstract"].apply(clean_text)
        df.loc[df["defense_date"].notnull(), "defense_date"] = df.loc[df["defense_date"].notnull(), "defense_date"].apply(convert_date)

        now = datetime.now()
        dt_string = now.strftime("%Y_%m_%d_%H_%M_%S")
        output_path = f"./data/ppgeec_{dt_string}.csv"

        try:
            df.to_csv(output_path, header=True, index=False, quoting=csv.QUOTE_ALL)
            logging.info(f"Data successfully saved to {output_path}")
        except Exception as e:
            logging.error(f"Failed to save data to CSV: {e}")
