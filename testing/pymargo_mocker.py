from __future__ import annotations

import cython
from cython.cimports.cpython.ref import PyObject

# pymargo vars
client = "client"
server = "server"

# server dictionary
data_dict = {}

class Engine:
    def __init__(self, url, mode=server):
        self.url = url
        pass

    def addr(self):
        return self.url
    
    def on_finalize(self, func):
        pass
    
    def enable_remote_shutdown(self):
        pass

    def wait_for_finalize(self):
        pass
    
    @cython.cfunc
    def create_bulk(self, data, bulk_type):
        return cython.cast(cython.pointer(PyObject), data)

    def lookup(self, addr):
        return self
    
    def shutdown(self):
        pass
    
    def finalize(self):
        pass
    
    def transfer(*args):
        pass
    
    def register(funcname, *args):
        return RPC(funcname)
    

class RPC:
    def __init__(self, name):
        self.name = name
    
    def on(self, addr):
        return self.mockfunc
    
    @cython.cfunc
    def mockfunc(self, array_str: cython.pointer(PyObject), size, key):
        if self.name == "set":
            data_dict[key] = array_str
            return "OK"
        elif self.name == "get":
            if key not in data_dict:
                return "ERROR"
            else:
                array_str = data_dict[key]
                print(array_str)
            return "OK"
        elif self.name == "evict":
            if key not in data_dict:
                return "ERROR"
            else:
                del data_dict[key]
            return "OK"
        else:
            array_str = key in data_dict
            return "OK"

class bulk:
    # bulk variable
    read_write = "rw" 
    write_only = "w"
    push = "push"
    pull = "pull"

class Bulk:
    def __init__():
        pass
    
class Handle:
    def __init__(self):
        pass
    
    def respond(self, text):
        return text

    def get_address(self):
        return "addr"
            

    