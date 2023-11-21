from .simple_node_store import *
from .base_node_store import *
from .remote_node_store import *
import random

port = random.randint(10000, 64000)

def get_node_store(name="simple", path="/tmp"):
  global port
  if name == "simple":
    return SimpleNodeStore(path)
  elif name == "remote":
    server = RemoteNodeStoreServer("localhost", port, path)
    server_thread = threading.Thread(target=server.Listen)
    server_thread.daemon = True
    server_thread.start()
    client = RemoteNodeStoreClient("localhost", port)
    port += 1
    return client