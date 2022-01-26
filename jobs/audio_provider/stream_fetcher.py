import os
import datetime
import logging
import json
from functools import partial
from multiprocessing.pool import Pool

import pandas as pd
from lxml import etree
import urllib.request as urlrequest
from model.stream_info import StreamInfo
from commons.db_service import (
    SQLAlchemyMySQLConnection, 
    LiquiBaseMySQLConnection, 
    BaseDBConnection
)

logger = logging.getLogger("stream_finder")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(process)d] %(message)s",
)

def acquire_stream_info_df(
            url: str, 
            ts: str,
            category: str = "Unclassified", 
            logger: logging.Logger = None
    ):
        try:
            page = urlrequest.urlopen(url).read()
            logger.info(f"<{category}> Parsed URL: {url}")
            html = etree.HTML(page)
            link_available_items = [
                item for item in 
                html.xpath("//table[@bgcolor='#EEEEEE']//td[@bgcolor='lightgreen']")
            ]
            available_links = [
                item.attrib["href"] for item in 
                html.xpath("//table[@bgcolor='#EEEEEE']//td[@bgcolor='lightgreen']//a[contains(@href, '/play/')]")
            ]
            assert len(link_available_items) == len(available_links)
            logger.info(f"<{category}> Fetched Items through URL: {url} ...")
            stream_info_ls = [
                StreamInfo.from_element(link, element, category, ts).dict()
                for element, link in zip(link_available_items, available_links)
            ]
            logger.info(f"<{category}> Fetched {len(link_available_items)} Available Stream Links")
            stream_info_df = pd.DataFrame.from_records(stream_info_ls)
            return stream_info_df
        except Exception as e:
            logger.error(f"<{category}> Failed on acquiring stream info from {url} due to {e}")
            return None

def main(
            crawl_urls_path: str, 
            liquibase_conn: BaseDBConnection = None,
            sqla_conn: BaseDBConnection = None,
            export_path: str = None
    ):
        ts = datetime.datetime.utcnow().isoformat()
        stream_df_collection = []
        logger.info(f"<Main> Start Fetching Stream Information")
        crawl_urls = json.load(open(crawl_urls_path, mode="r"))
        pool = Pool(processes=os.cpu_count())
        stream_df_collection = pool.starmap(
            partial(acquire_stream_info_df, logger=logger),
            [(url, ts, category) for category, url in crawl_urls.items()]
        )
        stream_final_df = pd.concat([meta_df for meta_df in stream_df_collection if meta_df is not None], axis=0)
        logger.info(f"<Main> Fetched {stream_final_df.shape[0]} Available Stream Links")
        if export_path is not None:
            stream_final_df.to_csv(export_path, index=False)
        if liquibase_conn is not None and sqla_conn is not None:
            liquibase_cursor = liquibase_conn.establish()
            sqla_cursor = sqla_conn.establish()
            # update schema
            # upload data
            pass

if __name__ == "__main__":

    main(
        crawl_urls_path="commons/crawl_urls.json",
        export_path="../liveatc/stream_info.csv"
    )
