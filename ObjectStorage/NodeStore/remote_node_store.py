import socketserver
import threading
import socket
import json

from .base_node_store import *
from .simple_node_store import *

def recv_all(sock):
  buff = b''
  while True:
    data = sock.recv(4096)
    buff += data
    if len(data) < 4096:
      break
  return buff

def GetHandler(node_store):
  class RemoteNodeStoreHandler(socketserver.BaseRequestHandler):
    def handle(self):
      data = recv_all(self.request)
      if len(data) <= 0:
        return
      data = str(data, 'utf-8')
      data = json.loads(data)
      if data['action'] == 'write':
        node_store.Write(data['key'], bytes(data['value']))
        self.request.sendall(bytes('ok', 'utf-8'))
      elif data['action'] == 'read':
        data = node_store.Read(data['key'])
        if data is not None:
          self.request.sendall(data)
      elif data['action'] == 'shutdown':
        self.server.shutdown()

  return RemoteNodeStoreHandler

class RemoteNodeStoreServer(BaseNodeStore):
  def __init__(self, host, port, path):
    self.host = host
    self.port = port
    self.node_store = SimpleNodeStore(path)
    self.server = socketserver.TCPServer((self.host, self.port), GetHandler(self.node_store))

  def Listen(self):
    self.server.serve_forever()

class RemoteNodeStoreClient(BaseNodeStore):
  def __init__(self, host, port):
    self.remote_port = port
    self.remote_host = host
  
  def Read(self, key):
    data = {
      'action': 'read',
      'key': key
    }
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.connect((self.remote_host, self.remote_port))
      sock.sendall(bytes(json.dumps(data), 'utf-8'))
      data = bytes(recv_all(sock))
      return data
  
  def Write(self, key, value):
    data = {
      'action': 'write',
      'key': key,
      'value': list(value),
    }
    str_data = json.dumps(data)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.connect((self.remote_host, self.remote_port))
      sock.sendall(bytes(str_data, 'utf-8'))
  
  def Close(self):
    data = {
      'action': 'shutdown'
    }
    str_data = json.dumps(data)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
      sock.connect((self.remote_host, self.remote_port))
      sock.sendall(bytes(str_data, 'utf-8'))

    