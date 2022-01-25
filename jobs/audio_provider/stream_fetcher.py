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

logger = logging.getLogger("stream_finder")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] [%(process)d] %(message)s",
)

def acquire_stream_info_df(
            url: str, 
            category: str = "Unclassified", 
            logger: logging.Logger = None
    ):
        page = urlrequest.urlopen(url).read()
        logger.info(f"<{category}> Parsed URL: {url}")
        html = etree.HTML(page)
        link_available_items = [
            item for item in 
            html.xpath("//table[@bgcolor='#EEEEEE']//td[@bgcolor='lightgreen']")
        ]
        logger.info(f"<{category}> Fetched Items through URL: {url} ...")
        stream_info_ls = [
            StreamInfo.from_element(element, category)
            for element in link_available_items
        ]
        logger.info(f"<{category}> Fetched {len(link_available_items)} Available Stream Links")
        stream_info_df = pd.DataFrame.from_records(stream_info_ls)
        stream_info_df["fetch-time"] = datetime.datetime.utcnow().isoformat()
        return stream_info_df

def main(
            crawl_urls_path: str = "commons/crawl_urls.json",
            stream_info_export_path: str = None,
    ):
        stream_df_collection = []
        logger.info(f"Start Fetching Stream Information")
        crawl_urls = json.load(open(crawl_urls_path, mode="r"))
        pool = Pool(processes=os.cpu_count())
        stream_df_collection = pool.starmap(
            partial(acquire_stream_info_df, logger=logger),
            [(url, category) for category, url in crawl_urls.items()]
        )
        stream_final_df = pd.concat([meta_df for meta_df in stream_df_collection], axis=0)
        logger.info(f"<Total> Fetched {stream_final_df.shape[0]} Available Stream Links")

if __name__ == "__main__":

    main()
