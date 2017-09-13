import sqlite3
import argparse
import pathlib
import datetime
import time

import requests
import mutagen.mp3

parser = argparse.ArgumentParser()
parser.add_argument('path', help="path to mp3s to update", type=pathlib.Path)

class Updater(object):
    USER_AGENT = 'dgilman get_album_art/1.0'
    URL_BASE = 'https://api.discogs.com'
    def __init__(self):
        user_token = open('user_token.txt').read().strip()

        self.args = parser.parse_args()
        self.session = requests.Session()
        self.session.headers.update({"Authorization": "Discogs token={0}".format(user_token),
            'User-Agent': self.USER_AGENT})
        self.api_req_reset = None

        self.conn = sqlite3.connect('cache.sqlite3')
        self.c = self.conn.cursor()

    def bump_req_time(self):
        self.api_req_reset = datetime.datetime.utcnow() + datetime.timedelta(seconds=60)

    def reset_req_count(self):
        self.api_req_remaining = 60

    def _request(self, url, params=None):
        now = datetime.datetime.utcnow()
        if self.api_req_reset == None or now > self.api_req_reset:
            self.bump_req_time()
            self.reset_req_count()

        if self.api_req_remaining == 0:
            print('sleeping')
            time.sleep((self.api_req_reset - now).total_seconds()+1)
            self.bump_req_time()

        # not sure what to do for errors here, so let's just roll...
        resp = self.session.get(url, params=params)
        self.api_req_remaining = resp.headers['X-Discogs-Ratelimit-Remaining']
        return resp.json()

    def get_artwork(self, artist, release_title):
        self.c.execute("SELECT image FROM cache WHERE artist = ? AND release_title = ?", (artist, release_title))
        cache = self.c.fetchall()
        if len(cache) != 0:
            print('cache hit')
            return cache[0][0]

        search_resp = self._request(self.URL_BASE + '/database/search', {"artist": artist, "release_title": release_title})
        masters = [x for x in search_resp['results'] if x['type'] == 'master']
        releases = [x for x in search_resp['results'] if x['type'] == 'release']
        if len(masters) > 0:
            master_id = masters[0]['id']
            release_resp = self._request(self.URL_BASE + '/masters/{0}'.format(master_id))
        elif len(releases) > 0:
            release_id = releases[0]['id']
            release_resp = self._request(self.URL_BASE + '/releases/{0}'.format(release_id))
        else:
            print('no search results found')
            return None

        primary_images = [x for x in release_resp['images'] if x['type'] == 'primary']
        secondary_images = [x for x in release_resp['images'] if x['type'] == 'secondary']
        if len(primary_images) > 0:
            image_url = primary_images[0]['uri']
        elif len(secondary_images) > 0:
            image_url = secondary_images[0]['uri']
        else:
            print('no primary images found')
            return None

        image = requests.get(image_url).content
        self.c.execute("INSERT INTO cache (artist, release_title, image) VALUES (?, ?, ?)", (artist, release_title, image))
        self.conn.commit()
        return image

    def get_album_art(self, mp3_path):
        mp3 = mutagen.mp3.MP3(mp3_path.as_posix())

        if len(mp3.tags.getall('APIC')) > 0:
            print('skipping', mp3_path.as_posix())
            return

        talbs = mp3.tags.getall('TALB')
        if len(talbs) == 0:
            return
        release_title = talbs[0].text[0]

        tpe1s = mp3.tags.getall('TPE1')
        if len(tpe1s) == 0:
            return
        artist = tpe1s[0].text[0]

        print(artist, release_title)
        artwork = self.get_artwork(artist, release_title)
        if artwork == None:
            print('no artwork found!')
            return
        #apic = mutagen.id3.APIC(mime='foo', type=3 (or maybe 0), data='bytes')
        #mp3.tags.add(apic)
        #mp3.save()
        apic = mutagen.id3.APIC(mime='image/jpeg', type=3, data=artwork)
        mp3.tags.add(apic)
        mp3.save()

    def run(self):
        for mp3_path in self.args.path.glob('**/*.mp3'):
            self.get_album_art(mp3_path)

def main():
    Updater().run()

if __name__ == "__main__":
    main()
