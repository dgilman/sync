import sqlite3
import argparse
import pathlib
import datetime
import time
import asyncio
import functools
import os
import subprocess

import requests
import mutagen.mp3
import mutagen.id3
import mutagen.mp4
import io

parser = argparse.ArgumentParser()
parser.add_argument('src', help="source path to mp3s", type=pathlib.Path)
parser.add_argument('dest', help="destination path of mp3s", type=pathlib.Path)


class InvalidMP3Tagging(Exception):
    pass


# Bizarrely, the fat32 supports UTF-8 but bans the classic reserved
# fat32 characters.
FAT32_REPLACEMENTS = {
    '"': "'",  # single apostrophe
    "*": "∗",  # ASTERISK OPERATOR
    "/": "∕",  # DIVISION SLASH
    ":": "∶",  # RATIO
    "<": "❮",  # HEAVY LEFT-POINTING ANGLE QUOTATION MARK ORNAMENT
    ">": "❯",  # HEAVY RIGHT-POINTING ANGLE QUOTATION MARK ORNAMENT
    "?": "ʔ",  # LATIN LETTER GLOTTAL STOP
    "+": "➕",  # HEAVY PLUS SIGN
    ",": "¸",  # CEDILLA
    ".": "․",  # ONE DOT LEADER
    ";": ";",  # GREEK QUESTION MARK
    "=": "゠",  # KATAKANA-HIRAGANA DOUBLE HYPHEN
    "[": "",
    "]": "",
}


def f(strng):
    """Clean up a string for filesystem usage.

    This replaces all periods in the string so it is not suitable for fixing up entire
    filenames, just parts of filenames."""
    return (''.join(FAT32_REPLACEMENTS.get(char, char) for char in strng))[:28].strip()


class Updater(object):
    USER_AGENT = 'dgilman get_album_art/1.0'
    URL_BASE = 'https://api.discogs.com'

    def __init__(self):
        user_token = os.environ['DISCOGS_USER_TOKEN']

        self.loop = asyncio.get_event_loop()
        self.read_q = asyncio.Queue(maxsize=10)
        self.write_q = asyncio.Queue(maxsize=10)

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
        self.negative_cache = set()

    def bump_req_time(self):
        self.api_req_reset = datetime.datetime.utcnow() + datetime.timedelta(seconds=60)

    def reset_req_count(self):
        self.api_req_remaining = 60

    async def _request(self, url, params=None):
        now = datetime.datetime.utcnow()
        if self.api_req_reset is None or now > self.api_req_reset:
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

        negative_cache_key = (artist, release_title)
        if negative_cache_key in self.negative_cache:
            return None

        search_resp = await self._request(self.URL_BASE + '/database/search', {"artist": artist, "release_title": release_title})
        masters = [x for x in search_resp['results'] if x['type'] == 'master']
        releases = [x for x in search_resp['results'] if x['type'] == 'release']
        if len(masters) > 0:
            image_url = masters[0]['thumb']
        elif len(releases) > 0:
            image_url = releases[0]['thumb']
        else:
            print(f'No search results found for {artist}: {release_title}')
            self.negative_cache.add(negative_cache_key)
            return None

        if not image_url:
            print(f'No thumbnail image for {artist}: {release_title}')
            self.negative_cache.add(negative_cache_key)
            return None

        if not (image_url.endswith('jpg') or image_url.endswith("jpeg")):
            print(f"Unknown URL type: {artist=} {release_title=} {image_url}")
            breakpoint()

        image = await self.loop.run_in_executor(None, self.session.get, image_url)
        image.raise_for_status()
        image = image.content
        self.c.execute("INSERT INTO cache (artist, release_title, image) VALUES (?, ?, ?)", (artist, release_title, image))
        self.conn.commit()
        return image

    async def process_music(self, blob: io.BytesIO, file_path: pathlib.Path):
        file_name = file_path.name.lower()
        if file_name.endswith('mp3'):
            return await self.process_mp3(blob, file_path)
        elif file_name.endswith('m4a'):
            return await self.process_m4a(blob, file_path)
        else:
            print(f"Unknown file suffix: {file_path.name}")

    async def process_mp3(self, blob: io.BytesIO, file_path: pathlib.Path):
        mp3 = mutagen.mp3.MP3(blob)

        talbs = mp3.tags.getall('TALB')
        if len(talbs) == 0:
            release_title = 'Unknown Album'
        else:
            release_title = talbs[0].text[0]

        tpe2s = mp3.tags.getall('TPE2')
        if len(tpe2s) > 0:
            # Use "Album Artist" if it exists for grouping
            artist = tpe2s[0].text[0]
        else:
            tpe1s = mp3.tags.getall('TPE1')
            if len(tpe1s) > 0:
                # Otherwise, use the normal artist field.
                artist = tpe1s[0].text[0]
            else:
                # Give up and use a default artist.
                artist = 'Unknown Artist'

        tit2s = mp3.tags.getall('TIT2')
        if len(tit2s) == 0:
            song_title = file_path.stem
        else:
            song_title = tit2s[0].text[0]
        song_title = f(song_title)

        def track_id(track_id_tag):
            if len(track_id_tag) == 0:
                retval = 1
            else:
                track_string = track_id_tag[0].text[0]
                if track_string == '/':
                    retval = 1
                elif '/' in track_string:
                    first, last = track_string.split('/')
                    retval = int(first)
                else:
                    retval = int(track_string)
            return retval

        track = track_id(mp3.tags.getall('TRCK'))
        tpos = track_id(mp3.tags.getall('TPOS'))

        dest_path = self.args.dest / f(artist) / f(release_title) / f"{tpos:02} - {track:02} - {song_title}.mp3"

        extra_apic_keys = [x for x in mp3.keys() if 'APIC' in x][1:]
        for extra_key in extra_apic_keys:
            del mp3[extra_key]

        apic = mp3.tags.getall('APIC')

        if len(apic) > 0:
            await self.process_apic(file_path, mp3, apic[0])
        else:
            await self.add_apic(file_path, mp3, artist, release_title)

        mp3.save(blob)
        return dest_path

    async def process_apic(self, file_path: pathlib.Path, mp3: mutagen.mp3.MP3, apic: mutagen.id3.APIC):
        if apic.data.startswith(b'\x89PNG'):
            input_format = 'png'
        elif apic.mime in ('image/jpeg', 'image/jpg'):
            input_format = "jpg"
        else:
            print("unknown file type")
            breakpoint()

        apic.type = mutagen.id3.PictureType.COVER_FRONT
        apic.mime = 'image/jpeg'

        proc = await asyncio.create_subprocess_exec("convert", f"{input_format}:-",
                                              "-resize", "300x300>",
                                              "-units", "PixelsPerInch",
                                              "-density", "72",
                                              "-quality", "80",
                                              "jpg:-", stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        stdout, _ = await proc.communicate(apic.data)
        if proc.returncode != 0:
            print(f"Bad convert: {file_path=}")
            breakpoint()

        apic.data = stdout

    async def add_apic(self, file_path: pathlib.Path, mp3: mutagen.mp3.MP3, artist: str, release_title: str):
        artwork = await self.get_artwork(artist, release_title)
        if artwork is None:
            return

        apic = mutagen.id3.APIC(mime='image/jpeg', type=3, data=artwork)
        mp3.tags.add(apic)

    async def process_m4a(self, blob: io.BytesIO, file_path: pathlib.Path):
        m4a = mutagen.mp4.MP4(blob)
        # XXX get the album artist too
        artist = m4a.tags.get('©ART', ['Unknown Artist'])
        artist = artist[0]
        album = m4a.tags.get('©alb', ['Unknown Album'])
        album = album[0]
        song_title = m4a.tags.get('©nam')
        song_title = f(song_title[0])

        track_info = m4a.tags.get('trkn')
        if len(track_info) == 0:
            track_no = 1
            track_cnt = 1
        else:
            track_no, track_cnt = track_info[0]

        dest_path = self.args.dest / f(artist) / f(album) / f"{track_no:02} - {track_cnt:02} - {song_title}.m4a"
        return dest_path

    async def list_files(self):
        with open('to_sync.txt', 'rt') as fd:
            lines = await self.loop.run_in_executor(None, fd.readlines)

            if len(lines) != len(set(lines)):
                print("Duplicates in to_sync.txt!")

            for line in lines:
                src_root = self.args.src / pathlib.Path(line.strip())
                if not src_root.exists():
                    print(f"Dir does not exist: {src_root}")

                m4a_paths = list(await self.loop.run_in_executor(None, src_root.glob, '**/*.m4a'))
                mp3_paths = list(await self.loop.run_in_executor(None, src_root.glob, '**/*.mp3'))

                src_paths = mp3_paths + m4a_paths
                src_paths = [x for x in src_paths if not x.name.startswith('.')]

                if len(src_paths) == 0:
                    print(f"No music files for {line}")

                for src_path in src_paths:
                    yield src_path

    async def get_files(self):
        async for src_path in self.list_files():
            blob = await self.loop.run_in_executor(None, src_path.read_bytes)
            await self.read_q.put((src_path, io.BytesIO(blob)))
        await self.read_q.put(None)

    async def process_files(self):
        while True:
            obj = await self.read_q.get()
            if obj is None:
                await self.write_q.put(None)
                return

            src_path, blob = obj

            try:
                dest_path = await self.process_music(blob, src_path)
            except Exception as e:
                import pdb; pdb.post_mortem()
                print(str(e), 'skipping', src_path.as_posix())
                continue

            if dest_path.exists():
                continue

            await self.write_q.put((blob, dest_path))

    async def write_files(self):
        while True:
            obj = await self.write_q.get()
            if obj is None:
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
