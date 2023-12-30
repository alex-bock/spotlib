
from ast import literal_eval
import os
from typing import Dict, List

import pandas as pd

from .connection import Connection


class Library:

    def __init__(self, cache_dir: str = ".library_cache/"):

        self._connection = Connection()

        self._cache_dir = cache_dir
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)

        self.artists = None
        self.albums = None
        self.tracks = None

        return

    def load_user_albums(
        self, query: bool = False, n: int = 1000, sort_by: str = None
    ):

        cache_fp = os.path.join(self._cache_dir, "albums.csv")
        if query or not os.path.exists(cache_fp):
            records = self._connection.query_user_albums(n=n)
            self.albums = self._parse_album_records(records)
            self.albums.set_index("id", inplace=True)
            self.albums.to_csv(cache_fp)
        else:
            if self.albums is None:
                self.albums = pd.read_csv(
                    cache_fp,
                    converters={
                        "secondary_artists": literal_eval,
                        "genres": literal_eval
                    }
                ).set_index("id")

        if sort_by is not None:
            self.albums.sort_values(sort_by, inplace=True)

        return

    def _parse_album_records(self, album_records: List[Dict]) -> pd.DataFrame:

        albums = []

        for album_record in album_records:
            album = {}
            album["id"] = album_record["album"]["id"]
            album["title"] = album_record["album"]["name"]
            album["type"] = album_record["album"]["album_type"]
            album["artist"] = album_record["album"]["artists"][0]["name"]
            album["secondary_artists"] = list(set([
                artist_record["name"]
                for artist_record in album_record["album"]["artists"][1:]
            ])) + list(set([
                artist_record["name"]
                for track in album_record["album"]["tracks"]["items"]
                for artist_record in track["artists"]
                if artist_record["name"] not in album["artist"]
            ]))
            album["genres"] = album_record["album"]["genres"]
            album["released"] = pd.to_datetime(
                album_record["album"]["release_date"]
            )
            album["cover_url"] = album_record["album"]["images"][0]["url"]
            albums.append(album)

        return pd.DataFrame(albums)

    def get_albums_by_artist(
        self, artist: str, include_secondary: bool = False
    ) -> pd.DataFrame:

        if include_secondary:
            return self.albums[
                (self.albums.artist == artist) |
                (self.albums.secondary_artists.apply(lambda x: artist in x))
            ]
        else:
            return self.albums[self.albums.artist == artist]
