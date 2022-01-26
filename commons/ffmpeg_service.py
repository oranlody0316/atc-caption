import os
import datetime
import logging
from typing import Any, Optional

import numpy as np
import ffmpeg
import sounddevice as sd
from scipy.io import wavfile
class FFmpegAudioStream(object):
    
    url: str
    flag: str
    probe = Optional[Any]
    stream = Optional[Any]
    codec_type = Optional[str]
    channels = Optional[int]
    samplerate = Optional[int]
    
    def __init__(self, url: str, flag: str = None, logger:logging.Logger = None):
        self.url = url
        self.flag = flag or url.split("/")[-1]
        self.logger = logger or logging.getLogger("AudioStream")
        self._establish()

    def describe(self):
        display_info = {
            "url": self.url,
            "flag": self.flag,
            "codec_type": self.codec_type,
            "channels": self.channels,
            "samplerate": self.samplerate
        }
        self.logger.info(f"[{self.flag}] Acquired Stream Info: {display_info}")
        return

    def _establish(self):
        try:
            probe = ffmpeg.probe(
                self.url, loglevel='error'
            )
            self.logger.info(f"[{self.flag}] Acquired Probe Info")
        except ffmpeg.Error as e:
            self.logger.error(f"[{self.flag}] Probe not available due to ffmpeg Exception: {e.stderr}")
        except BaseException as e:
            self.logger.error(f"[{self.flag}] Probe not available due to Exception: {e}")
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
            if self.channels is not None:
                self.channels = int(self.channels)
            if self.samplerate is not None:
                self.samplerate = int(self.samplerate)
            self.describe()
        finally:
            return self
    
    def stream_with_resolution(
                self, 
                export_dir: str = None, 
                patient_frame: int = 3,
                playback: bool = False,
        ):
            process = ffmpeg.input(
                self.url
            ).output(
                'pipe:',
                format='f32le',
                acodec='pcm_f32le',
                ac=self.channels,
                ar=self.samplerate,
                loglevel='quiet',
            ).run_async(pipe_stdout=True)
            # debug
            read_size = self.channels * self.samplerate * 4
            wait_frame = -1 # avoid triggering wait_frame == 0 for the first time
            record_frame_ls = []
            while True:
                buffer_arr = np.frombuffer(process.stdout.read(read_size), dtype=np.float32)
                avg_fluctuation = np.round(np.mean(np.abs(buffer_arr)), 3)
                if avg_fluctuation > 0:
                    wait_frame = patient_frame
                    record_frame_ls += [buffer_arr]
                    self.logger.debug(f"[{self.flag}] FD: {avg_fluctuation:.3f}, wait: {wait_frame}")
                else:
                    wait_frame -= 1
                    if wait_frame > 0:
                        record_frame_ls += [buffer_arr]
                        self.logger.debug(f"[{self.flag}] FD: {avg_fluctuation:.3f}, wait: {wait_frame}")
                if wait_frame == 0:
                    self.logger.info(f"[{self.flag}] Captured {len(record_frame_ls)} frames audio")
                    data = np.concatenate(record_frame_ls, axis=0)
                    # clear the record_frame
                    record_frame_ls = []
                    end_time = datetime.datetime.now().isoformat()
                    if playback is True:
                        sd.play(data, self.samplerate, blocking=False)
                    if export_dir is not None:
                        export_path = os.path.join(export_dir, f"{self.flag}_{end_time}.wav")
                        os.makedirs(export_dir, exist_ok=True)
                        self.logger.info(f"[{self.flag}] Exporting to {export_path}")
                        wavfile.write(filename=export_path, data=data, rate=self.samplerate)
    
if __name__ == "__main__":
    logger = logging.getLogger("stream_finder")
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s [%(levelname)s] [%(process)d] %(message)s",
    )
    stream = FFmpegAudioStream(
        url = "http://d.liveatc.net/kbos_twr",
        logger=logger
    )
    stream.stream_with_resolution(
        playback=False,
        export_dir="../liveatc/audios/"
    )
    pass