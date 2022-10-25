from __future__ import annotations
from copyreg import pickle

from pickle import loads
from typing import Any


data = {}

class MockEndpoint:
    """Mock Endpoint"""
    
    last_event: str
    key: str
    response: str

    def __init__(self):
        self.key = None
        self.last_event = None
        self.response = None
    
    async def send_obj(self, obj):
        event = loads(obj)
        
        if event['op'] == "set":
            data[event['key']] = event['data']
            
        self.key = event['key']
        self.last_event = event['op']
    
    async def recv_obj(self):
        if self.last_event == "get":
            try:
                return data[self.key]
            except KeyError:
                return None
        elif self.last_event == "exists":
            return self.key in data
        elif self.last_event == "evict":
            try:
                del data[self.key]
            except KeyError:
                pass
            return None
        return True

    async def close(self):
        return None

    def closed(self):
        return True
    
    
class MockUCP:
    """Mock UCP"""
    
    def get_address(ifname: str) -> str:
        return ifname
    
    def create_listener(handler: Any, port: int) -> Any:
        class Listener:
            def __init__(self):
                pass
            def closed():
                True
        return Listener()
    
    async def create_endpoint(host, port):
        return MockEndpoint()