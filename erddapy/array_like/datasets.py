"""Classes to represent ERDDAP datasets."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Dict, Union

import pandas as pd

from erddapy.array_like.connection import ERDDAPConnection
from erddapy.core.url import (
    _extract_url_components,
    get_download_url,
    get_info_url,
    urlopen,
)

StrLike = Union[str, bytes]
FilePath = Union[str, Path]


class ERDDAPDataset:
    """Base class for more focused table or grid datasets."""

    def __init__(
        self,
        dataset_id: str,
        connection: str | ERDDAPConnection,
        variables=None,
        constraints=None,
        extension="htmlTable",
    ):
        """Initialize instance of ERDDAPDataset."""
        self.dataset_id = dataset_id
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(connection))
        self._variables = variables
        self._constraints = constraints
        self._extension = extension
        self._meta = None

    def __getitem__():
        pass

    @property
    def extension(self) -> str:
        """Access private ._extension variable."""
        return self._extension

    @property
    def connection(self) -> ERDDAPConnection:
        """Access private ._connection variable."""
        return self._connection

    @connection.setter
    def connection(self, value: str | ERDDAPConnection):
        """Set private ._connection variable."""
        self._connection = ERDDAPConnection(ERDDAPConnection.to_string(value))

    def get(self, file_type: str) -> StrLike:
        """Request data using underlying connection."""
        pass

    def get_download_url(self) -> str:
        protocol = {
            TableDataset: "tabledap",
            GridDataset: "griddap",
            ERDDAPDataset: None,
        }
        url = get_download_url(
            server=self.connection.server,
            dataset_id=self.dataset_id,
            protocol=protocol[self.__class__],
            variables=self.variables,
            constraints=self.constraints,
            response=self.extension,
        )
        return url.rstrip("?")

    def open(self, file_type: str) -> FilePath:
        """Download and open dataset using underlying connection."""
        return self.connection.open(file_type)

    @lru_cache(maxsize=None)
    def get_meta(self):
        """Request dataset metadata from the server."""
        url = get_info_url(self.connection.server, self.dataset_id, "csv")
        data = urlopen(url)
        _df = pd.read_csv(data)
        meta = dict()
        for variable in set(_df["Variable Name"]):
            attributes = (
                _df.loc[_df["Variable Name"] == variable, ["Attribute Name", "Value"]]
                .set_index("Attribute Name")
                .to_dict()["Value"]
            )
            meta.update({variable: attributes})
        return meta

    @property
    def meta(self):
        """Access private ._meta attribute. Request metadata if ._meta is empty."""
        return self.get_meta() if (self._meta is None) else self._meta

    @property
    def variables(self):
        """Access private ._variables attribute."""
        if self._variables is None:
            _meta = self.meta
            self._variables = list(_meta.keys())
        return self._variables

    @property
    def constraints(self):
        """Access private ._constraints attribute."""
        if self._constraints is None:
            self._constraints = dict()
            for key, value in self.meta.items():
                if "actual_range" in value.keys():
                    (
                        self._constraints[">=" + key],
                        self._constraints["<=" + key],
                    ) = value["actual_range"].split(", ")
        return self._constraints

    @staticmethod
    def from_url(url: str | ERDDAPConnection, **kwargs) -> ERDDAPDataset:
        """Loads a dataset from a specific URL."""
        if isinstance(url, str):
            url = ERDDAPConnection(url)
        elif isinstance(url, ERDDAPConnection):
            pass
        else:
            raise TypeError(
                f"Value '{url}' must be a string, it is of type '{type(url)}'.",
            )

        url_dict = _extract_url_components(url.server)

        # Select class to instantiate depending on the protocol
        dataset_class = {"tabledap": TableDataset, "griddap": GridDataset}
        return dataset_class.get(url_dict["protocol"], ERDDAPDataset)(
            connection=url_dict["server"],
            dataset_id=url_dict["dataset_id"],
            extension=url_dict["extension"],
        )

    def url_segment(self, file_type: str) -> str:
        """Return URL segment without the base URL (the portion after 'https://server.com/erddap/')."""

    def url(self, file_type: str) -> str:
        """
        Return a URL constructed using the underlying ERDDAPConnection.

        The URL will contain information regarding the base class server info, the dataset ID,
        access method (tabledap/griddap), file type, variables, and constraints.

        This allows ERDDAPDataset subclasses to be used as more opinionated URL constructors while still
        not tying users to a specific IO method.

        Not guaranteed to capture all the specifics of formatting a request, such as if a server requires
        specific auth or headers.
        """
        pass

    def to_dataset(self):
        """Open the dataset as xarray dataset by downloading a subset NetCDF."""
        pass

    def opendap_dataset(self):
        """Open the full dataset in xarray via OpenDAP."""
        pass


class TableDataset(ERDDAPDataset):
    """Subclass of ERDDAPDataset specific to TableDAP datasets."""

    def to_dataframe(self):
        """Open the dataset as a Pandas DataFrame."""


class GridDataset(ERDDAPDataset):
    """Subclass of ERDDAPDataset specific to GridDAP datasets."""

    pass
