
from dotenv import load_dotenv

import spotipy as sp
from spotipy.oauth2 import SpotifyOAuth


load_dotenv()

DEFAULT_SCOPE = "user-library-read"
SPOTIPY_LIMIT = 20


class Connection:

    def __init__(self, scope: str = DEFAULT_SCOPE):

        self._scope = scope

        self._auth_manager = SpotifyOAuth(scope=self._scope)
        self._connection = sp.Spotify(auth_manager=self._auth_manager)

        return

    def query_user_albums(self, n: int = 1000):

        records = []
        offset = 0
        batch_size = SPOTIPY_LIMIT
        n_records = 0

        while batch_size > 0 and n_records <= n:
            batch = self._query_album_batch(n=SPOTIPY_LIMIT, offset=offset)
            batch_size = len(batch)
            if batch_size > 0:
                records.extend(batch)
            n_records += batch_size
            offset += SPOTIPY_LIMIT

        return records

    def _query_album_batch(self, offset: int, n: int = SPOTIPY_LIMIT):

        result = self._connection.current_user_saved_albums(
            limit=n, offset=offset
        )

        return result["items"]
