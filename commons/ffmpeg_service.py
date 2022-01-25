from lib2to3.pgen2.token import OP
from sys import int_info
import ffmpeg
import os
import datetime
import logging
from typing import Any, Optional

class FFmpegAudioStream(object):
    
    url: str
    flag: str
    probe = Optional[Any]
    stream = Optional[Any]
    codec_type = Optional[str]
    channels = Optional[int]
    samplerate = Optional[int]
    
    def __init__(self, url, flag=None):
        self.url = url
        self.flag = flag
        self.establish(url, flag)

    def describe(self):
        display_info = {
            "target_url": self.url,
            "flag": self.flag,
            "codec_type": self.codec_type,
            "channels": self.channels,
            "samplerate": self.samplerate
        }
        logging.info(f"[{self.flag}] Acquired Stream Info: {display_info}")
        return

    def establish(self, url, flag=None):
        try:
            probe = ffmpeg.probe(
                self.url, loglevel='error'
            )
        except ffmpeg.Error as e:
            logging.info(f"[{self.flag}] Stream <{self.url}> is not available due to ffmpeg Exception: {e.stderr}")
        except BaseException as e:
            logging.info(f"[{self.flag}] Stream <{self.url}> is not available due to Exception: {e}")
        else:
            self.probe = probe
            streams = self.probe.get("streams", [])
            assert len(streams) == 1, \
                f'[{self.flag}] Ternimated due to: there must be exactly one stream available'
            self.stream = streams[0]
            self.codec_type = self.stream.get("codec_type", None)
            assert self.codec_type == 'audio', \
                f'[{self.flag}] Ternimated due to: The stream must be an audio stream'
            self.channels = self.stream.get("channels", None)
            self.samplerate = self.stream.get('sample_rate', None)
            self.describe()
        finally:
            return self

    def get_record_cmd(self, runtime, export_dir):
        current_time = datetime.datetime.now()
        start_time_stamp = current_time.strftime("%Y%m%d%H%M%S%f")[:-3]
        duration = datetime.timedelta(seconds=runtime)
        duration_time_stamp = str(duration)
        end_time = current_time + duration
        end_time_stamp = end_time.strftime("%Y%m%d%H%M%S%f")[:-3]
        export_path = os.path.join(export_dir, f"{self.flag}-{start_time_stamp}-{end_time_stamp}.mp3")
        cmd = " ".join([
            "ffmpeg",
            "-i", str(self.url),
            "-t", str(duration_time_stamp),
            "-ac", str(self.channels),
            "-ar", str(self.samplerate),
            "-loglevel", "error",
            export_path
        ])
        return cmd