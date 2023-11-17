import json
import os
import numpy as np

from .node_store import NodeStore
from .parity import GaloisField

CHUNK_SIZE = 16


class ObjectStore:
  def __init__(self, path="/tmp", node_num=5):
    self.path = path
    self.node_num = node_num
    self.meta = {}
    self.nodes = {}
    self.stripe_size = (node_num - 2) * CHUNK_SIZE
    self.gf = GaloisField(num_data_disk=self.node_num - 2, num_check_disk=2)
    self.__init()
    
  def __init(self):
    # create directory
    if not os.path.exists(self.path):
      os.makedirs(self.path)
    
    if not os.path.exists(os.path.join(self.path, 'obj_meta.json')):
      # new object store
      self.__new_store()
      with open(os.path.join(self.path, 'obj_meta.json'), 'w') as f:
        f.write(json.dumps(self.meta))
    else:
      self.__load_meta()
    pass
  
  def __new_store(self):
    self.meta['node_num'] = self.node_num
    self.meta['nodes'] = []
    self.meta['keys'] = {}
    for i in range(self.node_num):
      path = os.path.join(self.path, f"node_{i}")
      self.nodes[i] = NodeStore(path)
      self.meta['nodes'].append(path)
  
  def __load_meta(self):
    with open(os.path.join(self.path, 'obj_meta.json'), 'r') as f:
      s = f.read()
      self.meta = json.loads(s)
  
  def __compute_parity(self, content):    
    return self.gf.matmul(self.gf.vander, content)
  
  def WriteToStore(self, input_file_path, key):
    file_meta = {
      'data_nodes': [],
      'parity_nodes': [],
    }
    # randomly select two nodes as parity nodes
    node_ids = list(range(self.node_num))
    parity_nodes = np.random.choice(node_ids, size=2, replace=False)
    data_nodes = list(set(node_ids) - set(parity_nodes))
    file_meta['data_nodes'] = data_nodes
    file_meta['parity_nodes'] = parity_nodes.tolist()
    
    data = self.__distribute_data(input_file_path, file_meta)
    parity = self.__compute_parity(data)
    for i in range(len(data_nodes)):
      node_id = data_nodes[i]
      self.nodes[node_id].Write(key, bytes(data[i,:].tolist()))
    for i in range(len(parity_nodes)):
      node_id = parity_nodes[i]
      self.nodes[node_id].Write(key, bytes(parity[i,:].tolist()))
    
    self.meta['keys'][key] = file_meta
    return True

  def ReadFromStore(self, key, output_file_path):
    if key not in self.meta['keys']:
      return False

    key_meta = self.meta['keys'][key]
    data_nodes = key_meta['data_nodes']
    parity_nodes = key_meta['parity_nodes']
    file_size = key_meta['size']
    # detect aliveness
    alive_data_nodes = []
    corrupted_data_nodes = []
    alive_parity_nodes = []
    corrupted_parity_nodes = []
    for i in range(len(data_nodes)):
      node_id = data_nodes[i]
      if self.nodes[node_id].Alive():
        alive_data_nodes.append(node_id)
      else:
        corrupted_data_nodes.append(i)
    
    for i in range(len(parity_nodes)):
      node_id = parity_nodes[i]
      if self.nodes[node_id].Alive():
        alive_parity_nodes.append(node_id)
      else:
        corrupted_parity_nodes.append(i + self.node_num - 2)
    
    corrupted_disk_list = sorted(corrupted_data_nodes + corrupted_parity_nodes)
    
    if len(alive_data_nodes) == len(data_nodes):
      content = []
      for node_id in alive_data_nodes:
        content.append(self.nodes[node_id].Read(key))

      content = bytes(b''.join(content))[:file_size]
      self.__write_to_file(output_file_path, content)
      return True

    if len(alive_data_nodes) >= len(data_nodes) - 2:
      content = []
      parity = []
      for node_id in alive_data_nodes:
        content.append(list(self.nodes[node_id].Read(key)))
      for node_id in alive_parity_nodes:
        parity.append(list(self.nodes[node_id].Read(key)))
        
      A = np.concatenate([np.eye(self.node_num - 2, dtype=int), self.gf.vander], axis=0)
      A_ = np.delete(A, obj=corrupted_disk_list, axis=0)
      E_ = np.concatenate([np.asarray(content), np.asarray(parity)], axis=0)
      # print(A_.shape)
      # print(E_.shape)
      D = self.gf.matmul(self.gf.inverse(A_), E_)
      C = self.gf.matmul(self.gf.vander, D)
      E = np.concatenate([D, C], axis=0)
      
      rebuild_content = []
      for i in range(self.node_num - 2):
        rebuild_content.append(bytes(E[i,:].tolist()))

      rebuild_content = bytes(b''.join(rebuild_content))[:file_size]
      self.__write_to_file(output_file_path, rebuild_content)
      return True
    
    return False

  def Recover(self):
    # detect failure and recover
    pass
  
  def __write_to_file(self, file_path, content):
    with open(file_path, 'wb') as f:
      f.write(content)
      
  def __distribute_data(self, input_file_path, file_meta):
    with open(input_file_path, 'rb') as f:
      s = list(f.read())
      size = len(s)
      file_meta['size'] = size
      total_stripe = size // self.stripe_size + 1
      total_stripe_size = total_stripe * self.stripe_size
      s = s + [0] * (total_stripe_size - size) # padding
      s = np.asarray(s, dtype=int)
      s = s.reshape(self.node_num - 2, CHUNK_SIZE * total_stripe)
      return s
  
  def Close(self):
    with open(os.path.join(self.path, 'obj_meta.json'), 'w') as f:
      # print(self.meta)
      f.write(json.dumps(self.meta))
  
  def CorruptParityNode(self, key, num=1):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    assert(num <= 2)
    if num == 1:
      parity_nodes = obj_meta['parity_nodes']
      node_id = np.random.choice(parity_nodes, size=1)[0]
      self.nodes[node_id].Corrupt()
      return True
    else:
      parity_nodes = obj_meta['parity_nodes']
      for node_id in parity_nodes:
        self.nodes[node_id].Corrupt()
      return True
  
  def CorruptDataNode(self, key, num=1):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    assert(num <= self.node_num - 2)
    data_nodes = obj_meta['data_nodes']
    node_ids = np.random.choice(data_nodes, size=num, replace=False)
    for node_id in node_ids:
      self.nodes[node_id].Corrupt()
    return True

  def RecoverAll(self, key):
    if key not in self.meta['keys']:
      return False
    
    obj_meta = self.meta['keys'][key]
    data_nodes = obj_meta['data_nodes']
    parity_nodes = obj_meta['parity_nodes']
    for node_id in data_nodes:
      self.nodes[node_id].Recover()
    for node_id in parity_nodes:
      self.nodes[node_id].Recover()
    return True