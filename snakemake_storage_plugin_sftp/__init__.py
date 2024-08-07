from dataclasses import dataclass, field
from pathlib import PosixPath
from typing import Any, Iterable, List, Optional
from urllib.parse import urlparse

import sftpretty

from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import (
    IOCacheStorageInterface,
    get_constant_prefix,
)

# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    username: Optional[str] = field(
        default=None,
        metadata={
            "help": "SFTP username",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "help": "SFTP password",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )
    private_key: Optional[str] = field(
        default=None,
        metadata={
            "help": "SFTP private key file path",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )
    private_key_pass: Optional[str] = field(
        default=None,
        metadata={
            "help": "SFTP private key file password",
            # Optionally request that setting is also available for specification
            # via an environment variable. The variable will be named automatically as
            # SNAKEMAKE_<storage-plugin-name>_<param-name>, all upper case.
            # This mechanism should only be used for passwords, usernames, and other
            # credentials.
            # For other items, we rather recommend to let people use a profile
            # for setting defaults
            # (https://snakemake.readthedocs.io/en/stable/executing/cli.html#profiles).
            "env_var": True,
            # Optionally specify that setting is required when the executor is in use.
            "required": False,
        },
    )
    not_sync_mtime: bool = field(
        default=False,
        metadata={
            "help": "Do not synchronize mtime when storing files or dirs.",
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.conn_pool = dict()

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="sftp://ftpserver.com:22/myfile.txt",
                type=QueryType.ANY,
                description="A file on an sftp server. "
                "The port is optional and defaults to 22.",
            )
        ]

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 1.0

    def rate_limiter_key(self, query: str, operation: Operation):
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        parsed = urlparse(query)
        return parsed.netloc

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme == "sftp" and parsed.path:
            return StorageQueryValidationResult(valid=True, query=query)
        else:
            return StorageQueryValidationResult(
                valid=False,
                query=query,
                reason="Query does not start with sftp:// or does not contain a path "
                "to a file or directory.",
            )

    def list_objects(self, query: Any) -> Iterable[str]:
        """Return an iterator over all objects in the storage that match the query.

        This is optional and can raise a NotImplementedError() instead.
        """
        # TODO implement this
        raise NotImplementedError()

    def get_conn(self, hostname: str, port: Optional[int] = 22):
        key = hostname, port
        if key not in self.conn_pool:
            conn = sftpretty.Connection(
                host=hostname,
                port=port if port is not None else 22,
                username=self.settings.username,
                password=self.settings.password,
                private_key=self.settings.private_key,
                private_key_pass=self.settings.private_key_pass,
            )
            self.conn_pool[key] = conn
            return conn
        return self.conn_pool[key]


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.parsed_query = urlparse(self.query)
        self.conn = self.provider.get_conn(
            self.parsed_query.hostname, self.parsed_query.port
        )

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object.
        # TODO implement this
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        # This is optional and can be left as is
        return f"{self.parsed_query.netloc}/{self.parsed_query.path}"

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        return self.conn.exists(self.parsed_query.path)

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        return self.conn.lstat(self.parsed_query.path).st_mtime

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        return self.conn.stat(self.parsed_query.path).st_size

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        get = (
            self.conn.get_r
            if self.conn.isdir(self.parsed_query.path)
            else self.conn.get
        )
        get(self.parsed_query.path, self.local_path(), preserve_mtime=True)

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        put = self.conn.put_r if self.local_path().is_dir() else self.conn.put
        parents = str(PosixPath(self.parsed_query.path).parent)
        if parents != ".":
            self.conn.mkdir_p(parents)
        put(
            self.local_path(),
            self.parsed_query.path,
            preserve_mtime=not self.provider.settings.not_sync_mtime,
            confirm=True,
        )

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        remove = (
            self.conn.rmdir
            if self.conn.isdir(self.parsed_query.path)
            else self.conn.remove
        )
        remove(self.parsed_query.path)

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        prefix = get_constant_prefix(self.query, strip_incomplete_parts=True)

        # Search the entire directory tree for files and empty directories
        # starting with the prefix (which may be a file, directory, or might not exist).
        items = []
        visit_queue = [prefix]
        while visit_queue:
            current = visit_queue.pop()
            if self.conn.isfile(current):
                items.append(current)
            elif self.conn.isdir(current):
                # Keep searching non-empty directories, but yield empty ones.
                contents = self.conn.listdir(current)
                if contents:
                    current = PosixPath(current)
                    visit_queue.extend([str(current / child) for child in contents])
                else:
                    items.append(current)

        return items
