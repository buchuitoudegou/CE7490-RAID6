import os
import numpy as np

from .base_node_store import BaseNodeStore

class SimpleNodeStore(BaseNodeStore):
  def __init__(self, path=""):
    super().__init__()
    self.path = path
    self.alive = True
    if not os.path.exists(self.path):
      os.system('mkdir ' + self.path)

  def Write(self, key, content):
    obj_path = os.path.join(self.path, f"{key}.obj")
    with open(obj_path, 'wb+') as f:
      f.write(content)
  
  def Read(self, key):
    obj_path = os.path.join(self.path, f"{key}.obj")
    with open(obj_path, 'rb') as f:
      content = f.read()
    return content
  
  def Alive(self):
    return self.alive
  
  def Crash(self):
    self.alive = False
  
  def Recover(self):
    self.alive = True
    
  def Corrupt(self, key):
    obj_path = os.path.join(self.path, f"{key}.obj")
    b = b''
    with open(obj_path, 'rb') as f:
      b = list(f.read())
    with open(obj_path, 'wb') as f:
      np.random.shuffle(b)
      f.write(bytes(b))