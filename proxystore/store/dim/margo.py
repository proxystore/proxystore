#!/usr/bin/env python

import logging
from multiprocessing import Process
from time import sleep
from typing import Dict, Any, NamedTuple

import numpy as np

import pymargo
import pymargo.bulk as bulk
from pymargo.core import Engine, Handle
from pymargo.bulk import Bulk

import proxystore.utils as utils
from proxystore.store.base import Store
from proxystore.store.dim.utils import get_ip_address

class MargoStoreKey(NamedTuple):
    """Key to objects in a MargoStore"""
    
    margo_key: str
    obj_size: int
    peer: str
    
class MargoClient(Store[MargoStoreKey]):

    host: str
    addr: str
    protocol: str
    engine: Engine
    _server: Process
    _rpc: dict[str, Any]
    _logger: logging.Logger

    # TODO : make host optional and try to get infiniband path automatically
    def __init__(self, name: str, *, interface: str, port: int, protocol: str = "verbs", cache_size: int = 16, stats: bool = False, ) -> None:
        """Initialization of a Margo client to issue RPCs to the Margo server.
        This client will initialize a local Margo server (Peer service) that it will
        store data to.

        Args:
            name (str): name of the store instance.
            interface (str) -- The network interface to use
            port (int): the desired port for the Margo server
            protocol (str) -- The communication protocol to use (e.g., tcp, sockets, verbs). default = "verbs"
            cache_size (int): size of LRU cache (in # of objects). If 0,
                the cache is disabled. The cache is local to the Python
                process (default: 16).
            stats (bool): collect stats on store operations (default: False).
        """

        self._logger = logging.getLogger(type(self).__name__)
        self.protocol = protocol

        self.host = get_ip_address(interface)
        self.port = port

        self.addr = f"{self.protocol}://{self.host}:{port}"

        self._server = Process(target=self._start_server)
        self._server.start()

        # allocate some time to start the server process
        sleep(2)

        # start client
        self.engine = Engine(self.protocol, mode=pymargo.client)

        self._rpcs = {
            "set": self.engine.register("set"),
            "get": self.engine.register("get"),
            "exists": self.engine.register("exists"),
            "evict": self.engine.register("evict"),
        }

        super().__init__(name, cache_size=cache_size, stats=stats, kwargs= {"interface": interface, "port": self.port, "protocol": self.protocol})

    def _start_server(self):
        """Launch the local Margo server (Peer) process using set self.address and self.port.
        Creates a file in the self.peer_dir with its identity such that other clients can connect
        to extract data.

        """
        print(f"starting server {self.addr}")
        server_engine = Engine(self.addr)

        self._logger.info(
            "Server running at address %s",
            str(server_engine.addr())
        )

        server_engine.on_finalize(WhenFinalize)
        server_engine.enable_remote_shutdown()

        # create server
        MargoServer(server_engine)

        server_engine.wait_for_finalize()

    def create_key(self, obj: Any) -> MargoStoreKey:
        return MargoStoreKey(
            margo_key=utils.create_key(obj),
            obj_size=len(obj),
            peer=self.addr
        )

    def evict(self, key: MargoStoreKey) -> None:
        self._logger.debug("Client issuing an evict request on key %s", key)
        self.call_rpc_on(
            self.engine, key.peer, self._rpcs["evict"], "", key.margo_key, 0, self._logger
        )

    def exists(self, key: MargoStoreKey) -> bool:
        self._logger.debug("Client issuing an exists request on key %s", key)
        buff = bytes(1)  # equivalent to malloc

        blk = self.engine.create_bulk(buff, bulk.read_write)

        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs["exists"],
            blk,
            key.margo_key,
            len(buff),
            self._logger
        )
        return bool(int(buff.decode("utf-8")))

    def get_bytes(self, key: MargoStoreKey) -> bytes | None:
        self._logger.debug("Client issuing get request on key %s", key)

        buff = bytes(key.obj_size)
        blk = self.engine.create_bulk(buff, bulk.read_write)

        self.call_rpc_on(
            self.engine,
            key.peer,
            self._rpcs["get"],
            blk,
            key,
            key.obj_size,
            self._logger
        )

        return buff

    def set_bytes(self, key: MargoStoreKey, data: bytes) -> None:
        self._logger.debug("Client %s issuing set request on key %s", self.addr, key)
        blk = self.engine.create_bulk(data, bulk.read_write)
        ret = self.call_rpc_on(
            self.engine,
            self.addr,
            self._rpcs["set"],
            blk,
            key.margo_key,
            key.obj_size,
            self._logger
        )
        return ret

    def close(self) -> None:
        """Terminate Peer server process and remove server identity file created
        during the initialization.
        """
        self._logger.info("Clean up requested")
        self.engine.lookup(self.addr).shutdown()
        self.engine.finalize()
        self._server.terminate()

    @staticmethod
    def call_rpc_on(
        engine: Engine,
        addr: str,
        rpc: Any,
        array_str: str,
        key: str,
        size: int,
        logger: logging.Logger
    ) -> str:
        """Initiates the desired RPC call (specified within the rpc) on the specified provider

        Arguments:
            engine -- The client-side engine
            addr -- The address of Margo provider to access (e.g. tcp://172.21.2.203:6367)
            rpc -- the rpc to issue to the server
            array_str -- the serialized data/buffer to send to the server
            key -- the identifier of the data stored on the server
            size -- the size of the the data

        Returns:
            A string denoting whether the communication was successful

        """
        server_addr = engine.lookup(addr)
        result = rpc.on(server_addr)(array_str, size, key)

        if result == "ERROR":
            logger.error(f"Key {key} does not exist.")

        return result


class MargoServer:

    data: Dict[str, str]

    def __init__(self, engine: Engine) -> None:
        """Initialize the server and register all RPC calls

        Arguments:
            engine -- the server engine created at the specified network address
            provider_id -- the ID of the provider which will store the data

        """

        self._logger = logging.getLogger(type(self).__name__)

        self.engine = engine
        self.engine.register("get", self.get)
        self.engine.register("set", self.set)
        self.engine.register("exists", self.exists)
        self.engine.register("evict", self.evict)

    def set(self, handle: Handle, bulk_str: Bulk, bulk_size: int, key: str) -> None:
        """Obtain data from the client and store it in local dictionary

        Arguments:
            handle -- the client handle
            bulk_str -- JSON-formatted string containing the data key, filled buffer and size
        """
        self._logger.debug("Received set RPC for key %s.", key)

        local_buffer = bytes(bulk_size)
        try:
            localBulk = self.engine.create_bulk(local_buffer, bulk.write_only)
            self.engine.transfer(
                bulk.pull, handle.get_addr(), bulk_str, 0, localBulk, 0, bulk_size
            )
            self.data[key] = local_buffer
            handle.respond("OK")
        except Exception as error:
            self._logger.error("An exception was caught: %s", error)
            handle.respond("ERROR")

    def get(self, handle: Handle, bulk_str: Bulk, bulk_size: int, key: str) -> None:
        """Return data at a given key back to the client.

        Arguments:
            handle -- The client handle
            bulk_str -- a JSON-formatted string containing the key, data size and
            empty buffer to be filled.

        """
        self._logger.debug("Received get RPC for key %s.", key)

        try:
            localArray = self.data[key]
            localBulk = self.engine.create_bulk(localArray, bulk.read_only)
            self.engine.transfer(
                bulk.push, handle.get_addr(), bulk_str, 0, localBulk, 0, bulk_size
            )
            handle.respond("OK")
        except Exception as error:
            self._logger.error("An exception was caught: %s", error)
            handle.respond("ERROR")

    def evict(self, handle: Handle, bulk_str: str, bulk_size: int, key: str) -> None:
        """Remove key from local dictionary

        Arguments:
            handle -- the client issuing the requests' handle
            bulk_str -- A JSON-formatted string containing the key and buffer

        """
        self._logger.debug("Received exists RPC for key %s", key)

        try:
            del self.data[key]
            handle.respond("OK")
        except Exception as error:
            self._logger.error("An exception was caught: %s", error)
            handle.respond("ERROR")

    def exists(self, handle: Handle, bulk_str: str, bulk_size: int, key: str) -> None:
        """Verifies whether key exists within local dictionary

        Arguments:
            handle -- the client issuing the requests' handle
            bulk_str -- A JSON-formatted string containing the key and buffer

        """
        self._logger.debug("Received exists RPC for key %s", key)

        try:
            localArray = bytes(str(int(key in self.data)), "utf-8")
            localBulk = self.engine.create_bulk(localArray, bulk.read_only)
            size = len(localArray)
            self.engine.transfer(
                bulk.push, handle.get_addr(), bulk_str, 0, localBulk, 0, size
            )
            handle.respond("OK")
        except Exception as error:
            self._logger.error("An exception was caught: %s", error)
            handle.respond("ERROR")


def WhenFinalize() -> None:
    """Prints a statement advising that engine finalization was triggered"""
    print("Finalize was called. Cleaning up.")