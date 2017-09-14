import sqlite3
import argparse
import pathlib
import datetime
import time
import asyncio
import functools
import os

import requests
import mutagen.mp3
import io

parser = argparse.ArgumentParser()
parser.add_argument('src', help="source path to mp3s", type=pathlib.Path)
parser.add_argument('dest', help="destination path of mp3s", type=pathlib.Path)

class InvalidMP3Tagging(Exception): pass

def f(strng):
    """Clean up a string for filesystem usage."""
    return strng.replace('/', ' ')[:28]

class AsyncIteratorExecutor:
    """
    Converts a regular iterator into an asynchronous
    iterator, by executing the iterator in a thread.
    """
    def __init__(self, iterator, loop=None, executor=None):
        self.__iterator = iterator
        self.__loop = loop or asyncio.get_event_loop()
        self.__executor = executor

    def __aiter__(self):
        return self

    async def __anext__(self):
        value = await self.__loop.run_in_executor(
            self.__executor, next, self.__iterator, self)
        if value is self:
            raise StopAsyncIteration
        return value


class Updater(object):
    USER_AGENT = 'dgilman get_album_art/1.0'
    URL_BASE = 'https://api.discogs.com'
    def __init__(self):
        user_token = os.environ['DISCOGS_USER_TOKEN']

        self.loop = asyncio.get_event_loop()
        self.read_q = asyncio.Queue(loop=self.loop, maxsize=10)
        self.write_q = asyncio.Queue(loop=self.loop, maxsize=10)

        self.args = parser.parse_args()
        self.session = requests.Session()
        self.session.headers.update({"Authorization": "Discogs token={0}".format(user_token),
            'User-Agent': self.USER_AGENT})
        self.api_req_reset = None

        self.conn = sqlite3.connect('cache.sqlite3')
        self.c = self.conn.cursor()
        self.c.execute("""
CREATE TABLE IF NOT EXISTS cache (
   id INTEGER PRIMARY KEY,
   artist TEXT NOT NULL,
   release_title TEXT NOT NULL,
   image BLOB,
   unique (artist, release_title)
)""")

    def bump_req_time(self):
        self.api_req_reset = datetime.datetime.utcnow() + datetime.timedelta(seconds=60)

    def reset_req_count(self):
        self.api_req_remaining = 60

    async def _request(self, url, params=None):
        now = datetime.datetime.utcnow()
        if self.api_req_reset == None or now > self.api_req_reset:
            self.bump_req_time()
            self.reset_req_count()

        if self.api_req_remaining == 0:
            time.sleep((self.api_req_reset - now).total_seconds()+1)
            self.bump_req_time()

        # not sure what to do for errors here, so let's just roll...
        resp = await self.loop.run_in_executor(None, functools.partial(self.session.get, url, params=params))
        self.api_req_remaining = resp.headers['X-Discogs-Ratelimit-Remaining']
        return resp.json()

    async def get_artwork(self, artist, release_title):
        # unfortunately, the executor runs stuff in a different thread, which fucks with sqlite
        # We block the execution of the main thread but hey, it'll be fast, right?
        self.c.execute("SELECT image FROM cache WHERE artist = ? AND release_title = ?", (artist, release_title))
        cache = self.c.fetchall()
        if len(cache) != 0:
            return cache[0][0]

        search_resp = await self._request(self.URL_BASE + '/database/search', {"artist": artist, "release_title": release_title})
        masters = [x for x in search_resp['results'] if x['type'] == 'master']
        releases = [x for x in search_resp['results'] if x['type'] == 'release']
        if len(masters) > 0:
            image_url = masters[0]['thumb']
        elif len(releases) > 0:
            image_url = releases[0]['thumb']
        else:
            print(f'No search results found for {artist}: {release_title}')
            return None

        image = await self.loop.run_in_executor(None, requests.get, image_url)
        image = image.content
        self.c.execute("INSERT INTO cache (artist, release_title, image) VALUES (?, ?, ?)", (artist, release_title, image))
        self.conn.commit()
        return image

    async def process_mp3(self, blob, potential_song_title):
        mp3 = mutagen.mp3.MP3(blob)

        talbs = mp3.tags.getall('TALB')
        if len(talbs) == 0:
            release_title = 'Unknown Album'
        else:
            release_title = talbs[0].text[0]
        tpe1s = mp3.tags.getall('TPE1')

        if len(tpe1s) == 0:
            artist = 'Unknown Artist'
        else:
            artist = tpe1s[0].text[0]

        tit2s = mp3.tags.getall('TIT2')
        if len(tit2s) == 0:
            song_title = potential_song_title
        else:
            song_title = tit2s[0].text[0]
        song_title = f(song_title)

        trcks = mp3.tags.getall('TRCK')
        if len(trcks) == 0:
            track = 1
        else:
            track_string = trcks[0].text[0]
            if '/' in track_string:
                first, last = track_string.split('/')
                track = int(first)
            else:
                track = int(track_string)

        tpos = mp3.tags.getall('TPOS')
        if len(tpos) == 0:
            tpos = 1
        else:
            tpos = tpos[0].text[0]
            if '/' in tpos:
                first, last = tpos.split('/')
                tpos = int(first)
            else:
                tpos = int(tpos)

        dest_path = self.args.dest / f(artist) / f(release_title) / f"{tpos:02} - {track:02} - {song_title}.mp3"

        if len(mp3.tags.getall('APIC')) > 0:
            mp3.save(blob)
            return dest_path

        artwork = await self.get_artwork(artist, release_title)
        if artwork == None:
            mp3.save(blob)
            return dest_path
        apic = mutagen.id3.APIC(mime='image/jpeg', type=3, data=artwork)
        mp3.tags.add(apic)
        mp3.save(blob)
        return dest_path

    async def list_files(self):
        with open('to_sync.txt', 'rt') as fd:
            async for line in AsyncIteratorExecutor(fd):
                src_root = self.args.src / pathlib.Path(line[:-1])
                async for src_path in AsyncIteratorExecutor(src_root.glob('**/*.mp3')):
                    yield src_path

    async def get_files(self):
        async for src_path in self.list_files():
            with open(src_path, 'rb') as fd:
                blob = await self.loop.run_in_executor(None, fd.read)
                await self.read_q.put((src_path, io.BytesIO(blob)))
        await self.read_q.put(None)

    async def process_files(self):
        while True:
            obj = await self.read_q.get()
            if obj == None:
                await self.write_q.put(None)
                return
            src_path, blob = obj
            try:
                dest_path = await self.process_mp3(blob, src_path.stem)
            except Exception as e:
                print(str(e), 'skipping', src_path.as_posix())
                continue
            await self.write_q.put((blob, dest_path))

    async def write_files(self):
        while True:
            obj = await self.write_q.get()
            if obj == None:
                return
            blob, dest_path = obj
            blob.seek(0)
            await self.loop.run_in_executor(None, functools.partial(dest_path.parent.mkdir, parents=True, exist_ok=True))
            await self.loop.run_in_executor(None, dest_path.write_bytes, blob.read())

    def run(self):
        self.loop.run_until_complete(asyncio.gather(self.get_files(), self.process_files(), self.write_files()))
        self.loop.close()

def main():
    Updater().run()
    print("Dont forget to sudo fatsort -cn /dev/disk1s1")

if __name__ == "__main__":
    main()
