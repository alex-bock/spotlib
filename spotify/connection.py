
from dotenv import load_dotenv
from typing import Dict

import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()

DEFAULT_SCOPE = "user-library-read"
SPOTIPY_LIMIT = 20

DEFAULT_RECORD_LIMITS = {
    "album": 1000,
    "artist": 1000,
    "track": 5000
}


class Connection:

    def __init__(self, scope: str = DEFAULT_SCOPE):

        self._scope = scope

        self._auth_manager = SpotifyOAuth(scope=self._scope)
        self._connection = sp.Spotify(auth_manager=self._auth_manager)

        return

    def query_user_records(self, record_type: str, n: int = None):

        if n is None:
            n = DEFAULT_RECORD_LIMITS[record_type]

        records = []
        offset = 0
        batch_size = SPOTIPY_LIMIT
        n_records = 0

        while batch_size > 0 and n_records <= n:
            batch = self._query_record_batch(
                record_type, n=SPOTIPY_LIMIT, offset=offset
            )
            batch_size = len(batch)
            if batch_size > 0:
                records.extend(batch)
            n_records += batch_size
            offset += SPOTIPY_LIMIT

        return records

    def _query_record_batch(
        self, record_type: str, offset: int = 0, n: int = SPOTIPY_LIMIT
    ) -> Dict:

        if record_type == "album":
            result = self._connection.current_user_saved_albums(
                limit=n, offset=offset
            )
        elif record_type == "artist":
            raise NotImplementedError
        elif record_type == "track":
            result = self._connection.current_user_saved_tracks(
                limit=n, offset=offset
            )

        return result["items"]

    def query_artist(self, id: str) -> Dict:

        result = self._connection.artist(id)

        return result
