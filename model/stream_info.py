from pydantic import BaseModel
from typing import Optional, List
from lxml.etree import Element

class StreamInfo(BaseModel):
    flag: str
    stream_link: str
    abstract: Optional[str]
    category: str
    metar: Optional[str]
    place: str
    ts: str
    
    @classmethod
    def from_element(cls, link: str, element: Element, category: str, ts: str):
        text_ls = list(element.itertext())
        flag = cls.get_flag(link)
        stream_link = cls.flag_to_stream_link(flag)
        abstract = text_ls[0]
        place = text_ls[2]
        try:
            metar = text_ls[11]
            assert metar.isupper() is True
        except AssertionError as e:
            metar = "N/A"
        obj = cls(
            flag=flag,
            stream_link=stream_link,
            abstract=abstract,
            category=category,
            metar=metar,
            place=place,
            ts=ts
        )
        return obj
    
    @staticmethod 
    def get_flag(pls_link):
        return pls_link.strip("/play/").split(".")[0]

    @staticmethod
    def flag_to_stream_link(flag):
        return "http://d.liveatc.net/{}".format(flag)