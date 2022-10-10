"""Class ERDDAPConnection to represent connection to a particular URL."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Union

from erddapy.core.url import urlopen

StrLike = Union[str, bytes]
FilePath = Union[str, Path]


class ERDDAPConnection:
    """
    Manages connection that will be used in ERDDAPServer instances.

    While most ERDDAP servers allow connections via a bare url, some servers may require authentication
    to access data.
    """

    def __init__(self, server: str, auth: tuple | None = None):
        """Initialize instance of ERDDAPConnection."""
        self._server = self.to_string(server)
        self._auth = auth

    def __repr__(self) -> str:
        return f"<erddapy.array_like.connection.ERDDAPConnection to server '{self.server}'>"

    @property
    def auth(self) -> str:
        return self._auth

    @auth.setter
    def auth(self, value: str):
        self._auth = value

    @classmethod
    def to_string(cls, value):
        """Convert an instance of ERDDAPConnection to a string."""
        if isinstance(value, str):
            return value.rstrip("/")
        elif isinstance(value, cls):
            return value.server.rstrip("/")
        else:
            raise TypeError(
                f"Server must be either a string or an instance of ERDDAPConnection. '{value}' was "
                f"passed.",
            )

    def get(self, url_part: str, **kwargs) -> StrLike:
        """
        Request data from the server.

        Uses requests by default similar to most of the current erddapy data fetching functionality.

        Can be overridden to use httpx, and potentially aiohttp or other async functionality, which could
        hopefully make anything else async compatible.
        """
        return urlopen(url_part, self.auth, **kwargs)

    @contextmanager
    def open(self, url_part: str) -> FilePath:
        """Yield file-like object for access for file types that don't enjoy getting passed a string."""
        try:
            tmp = NamedTemporaryFile(suffix=".tmp", prefix="erddapy_")
            yield Path(tmp.name)
        finally:
            if tmp is not None:
                tmp.close()

    @property
    def server(self) -> str:
        """Access the private ._server attribute."""
        return self._server

    @server.setter
    def server(self, value: str):
        """Set private ._server attribute."""
        self._server = self.to_string(value)
