
from ast import literal_eval
import os
import requests
import time
from typing import Dict, List

import math
import pandas as pd
from PIL import Image, ImageStat
import plotly.graph_objects as go

from .connection import Connection
from .constants import DEFAULT_ALBUM_LIMIT, DEFAULT_TRACK_LIMIT


COLLAGE_COVER_DIM = 800


class Library:

    def __init__(self, cache_dir: str = ".library_cache/"):

        self._connection = Connection()

        self._cache_dir = cache_dir
        if not os.path.exists(self._cache_dir):
            os.makedirs(self._cache_dir)

        self.artists = None
        self.albums = None
        self.tracks = None

        self.cover_dir = os.path.join(self._cache_dir, "album_covers/")
        self.collage_dir = os.path.join(self._cache_dir, "album_collages/")

        if not os.path.exists(self.cover_dir):
            os.makedirs(self.cover_dir)
        if not os.path.exists(self.collage_dir):
            os.makedirs(self.collage_dir)

        return

    def load_user_albums(
        self,
        query: bool = False,
        n: int = DEFAULT_ALBUM_LIMIT,
        sort_by: str = None
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

    def load_user_tracks(
        self,
        query: bool = False,
        n: int = DEFAULT_TRACK_LIMIT,
        sort_by: str = None
    ):

        cache_fp = os.path.join(self._cache_dir, "tracks.csv")
        if query or not os.path.exists(cache_fp):
            records = self._connection.query_user_tracks(n=n)
            self.tracks = self._parse_track_records(records)
            self.tracks.set_index("id", inplace=True)
            self.tracks.to_csv(cache_fp)
        else:
            if self.tracks is None:
                self.tracks = pd.read_csv(
                    cache_fp,
                    converters={
                        "artists": literal_eval
                    }
                ).set_index("id")

        if sort_by is not None:
            self.tracks.sort_values(sort_by, inplace=True)

        return

    def _parse_track_records(self, track_records: List[Dict]) -> pd.DataFrame:

        tracks = []

        for track_record in track_records:
            track = {}
            track["id"] = track_record["track"]["id"]
            track["title"] = track_record["track"]["name"]
            track["artists"] = list(set([
                artist_record["name"]
                for artist_record in track_record["track"]["artists"]
            ]))
            track["album"] = track_record["track"]["album"]["name"]
            track["album_id"] = track_record["track"]["album"]["id"]
            track["released"] = pd.to_datetime(
                track_record["track"]["album"]["release_date"]
            )
            track["genres"] = list(set([
                genre for artist_record in [
                    self._connection.query_artist(artist["id"])
                    for artist in track_record["track"]["artists"]
                ]
                for genre in artist_record["genres"]
            ]))
            tracks.append(track)

        return pd.DataFrame(tracks)

    def get_albums_by_artist(
        self, artist: str, include_secondary: bool = False
    ) -> pd.DataFrame:

        if include_secondary:
            secondary_mask = (
                self.albums.secondary_artists.apply(lambda x: artist in x)
            )
        else:
            secondary_mask = pd.Series([False] * len(self.albums))

        return self.albums[(self.albums.artist == artist) | secondary_mask]

    def view_albums_by_release_date(self, year_res: float = 1.0):

        fig = go.Figure(
            data=[
                go.Histogram(
                    x=self.albums.released,
                    xbins={
                        "start": self.albums.released.min(),
                        "end": self.albums.released.max(),
                        "size": "M" + str(int(year_res * 12))
                    }
                )
            ]
        )
        fig.show()

        return

    def generate_album_collage(
        self,
        dim: int = 10,
        sort_by: str = None,
        start_date: str = None,
        end_date: str = None,
        gradient: bool = False,
        fn: str = None
    ):

        n = dim ** 2
        albums = self.albums

        if start_date is not None:
            albums = albums[albums.released >= start_date]
        if end_date is not None:
            albums = albums[albums.released < end_date]
        if len(albums) < n:
            dim = math.floor(math.log2(len(albums)))
            n = dim ** 2

        if sort_by:
            albums = albums.sort_values(sort_by).head(n)
        else:
            albums = albums.sample(n)

        self._pull_album_covers(albums)
        albums["cover_img"] = self._load_cover_images(albums)

        if gradient:
            albums["cover_brightness"] = albums.cover_img.apply(
                lambda x: ImageStat.Stat(x.convert("L")).mean[0]
            )
            albums.sort_values(
                "cover_brightness", inplace=True, ascending=False
            )

        collage = self._build_collage_image(albums, dim)

        if fn is None:
            fn = f"{str(int(time.time()))}.png"
        collage.save(os.path.join(self.collage_dir, fn))

        return
    
    def _pull_album_covers(self, albums: pd.DataFrame):

        for id, album in albums.iterrows():
            cover_fp = os.path.join(self.cover_dir, f"{id}.png")
            if not os.path.exists(cover_fp):
                with open(cover_fp, "wb") as f:
                    f.write(requests.get(album.cover_url).content)
        
        return
    
    def _load_cover_images(self, albums: pd.DataFrame) -> pd.Series:

        cover_imgs = albums.index.map(
            lambda x: Image.open(
                os.path.join(self.cover_dir, f"{x}.png")
            ).resize(
                (COLLAGE_COVER_DIM, COLLAGE_COVER_DIM)
            )
        )

        return cover_imgs
    
    def _build_collage_image(self, albums: pd.DataFrame, dim: int) -> Image:

        collage_img = Image.new(
            "RGB", (COLLAGE_COVER_DIM * dim, COLLAGE_COVER_DIM * dim)
        )
        for i in range(dim):
            for j in range(dim):
                collage_img.paste(
                    albums.iloc[(i * dim) + j].cover_img,
                    (COLLAGE_COVER_DIM * j, COLLAGE_COVER_DIM * i)
                )

        return collage_img

    def get_tracks_by_artist(self, artist: str) -> pd.DataFrame:

        return self.tracks[self.tracks.artists.apply(lambda x: artist in x)]

    def get_tracks_from_album(
        self, album: str, artist: str = None
    ) -> pd.DataFrame:

        if artist is None:
            album_id = self.albums[self.albums.title == album].iloc(0).album_id
        else:
            artist_albums = self.get_albums_by_artist(artist)
            album_id = artist_albums[
                artist_albums.title == album
            ].iloc(0).album_id

        return self.tracks(self.tracks.album_id == album_id)

    def view_tracks_by_release_date(self, year_res: int = 1.0):

        fig = go.Figure(
            data=[
                go.Histogram(
                    x=self.tracks.released,
                    xbins={
                        "start": self.tracks.released.min(),
                        "end": self.tracks.released.max(),
                        "size": "M" + str(int(year_res * 12))
                    }
                )
            ]
        )
        fig.show()

        return
