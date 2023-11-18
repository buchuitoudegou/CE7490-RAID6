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
  
  def __detect_data_corruption(self, content, size, parity):
    dcontent = self.__pad_and_reshape(list(content), size)
    dparity = self.__compute_parity(dcontent)
    corrupt = []
    for i in range(len(parity)):
      if parity[i] != bytes(dparity[i,:].tolist()):
        corrupt.append(i)
    return corrupt
    
  
  def WriteToStore(self, input_file_path, key):
    file_meta = {
      'data_nodes': [],
      'parity_nodes': [],
      'error': 'No',
    }
    # randomly select two nodes as parity nodes
    node_ids = list(range(self.node_num))
    parity_nodes = np.sort(np.random.choice(node_ids, size=2, replace=False))
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
      content, parity = [], []
      for node_id in alive_data_nodes:
        content.append(self.nodes[node_id].Read(key))

      for node_id in alive_parity_nodes:
        parity.append(self.nodes[node_id].Read(key))

      bcontent = bytes(b''.join(content))[:file_size]
      corrupt = []
      if len(alive_parity_nodes) == 2:
        # if the parity node crashes, we don't know which disk is corrupted
        corrupt = self.__detect_data_corruption(bcontent, file_size, parity)
      parity = [list(parity[i]) for i in range(len(parity))]
      content = [list(content[i]) for i in range(len(content))]
      if len(corrupt) == 0:
        self.__write_to_file(output_file_path, bcontent)
      elif len(corrupt) == 1:
        # parity driven corruption
        self.meta['keys'][key]['error'] = 'Parity'
        self.__write_to_file(output_file_path, bcontent)
      else:
        # data driven corruption
        # TODO: detect which disk is corrupted
        self.meta['keys'][key]['error'] = 'Data'
        corrupted_disk_list.append(0)
        content.pop(corrupted_disk_list[0])
        rebuild_content = self.__data_rebuild(content, parity, corrupted_disk_list, file_size)
        self.__write_to_file(output_file_path, rebuild_content)
      return True

    if len(alive_data_nodes) >= len(data_nodes) - 2:
      # erasure failure
      content = []
      parity = []
      for node_id in alive_data_nodes:
        content.append(list(self.nodes[node_id].Read(key)))
      for node_id in alive_parity_nodes:
        parity.append(list(self.nodes[node_id].Read(key)))
      # print('corrupted: ', corrupted_disk_list)
      rebuild_content = self.__data_rebuild(content, parity, corrupted_disk_list, file_size)
      self.__write_to_file(output_file_path, rebuild_content)
      return True
    
    return False

  def __data_rebuild(self, content, parity, corrupted_disk_list, file_size):
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
    return rebuild_content

  def RecoverCorruptedData(self):
    for key in self.meta['keys']:
      key_meta = self.meta['keys'][key]
      if self.meta['keys'][key]['error'] == 'Data':
        corrupted_disk_list = [0]
        content, parity = [], []
        idx = 0
        for node_id in key_meta['data_nodes']:
          if idx == 0:
            idx += 1
            continue
          content.append(list(self.nodes[node_id].Read(key)))
        for node_id in key_meta['parity_nodes']:
          parity.append(list(self.nodes[node_id].Read(key)))
        rebuild_content = self.__data_rebuild(content, parity, corrupted_disk_list, key_meta['size'])
        real_content = self.__distribute_real_data(rebuild_content)
        self.nodes[key_meta['data_nodes'][0]].Write(key, bytes(real_content[0,:].tolist()))
        self.meta['keys'][key]['error'] = 'No'
      elif self.meta['keys'][key]['error'] == 'Parity':
        content, parity = [], []
        for node_id in key_meta['data_nodes']:
          content.append(list(self.nodes[node_id].Read(key)))
        content = np.asarray(content).reshape(-1).tolist()
        parity = self.__compute_parity(self.__pad_and_reshape(content, len(content)))
        idx = 0
        for node_id in key_meta['parity_nodes']:
          self.nodes[node_id].Write(key, bytes(parity[idx,:].tolist()))
          idx += 1
        self.meta['keys'][key]['error'] = 'No'
  
  def __write_to_file(self, file_path, content):
    with open(file_path, 'wb') as f:
      f.write(content)
      
  def __distribute_data(self, input_file_path, file_meta):
    with open(input_file_path, 'rb') as f:
      s = list(f.read())
      size = len(s)
      file_meta['size'] = size
      return self.__pad_and_reshape(s, size)
  
  def __distribute_real_data(self, data):
    s = list(data)
    return self.__pad_and_reshape(s, len(s))
  
  def __pad_and_reshape(self, s, size):
    total_stripe = size // self.stripe_size
    if size % self.stripe_size != 0:
      total_stripe += 1
    total_stripe_size = total_stripe * self.stripe_size
    s = s + [0] * (total_stripe_size - size) # padding
    s = np.asarray(s, dtype=int)
    s = s.reshape(self.node_num - 2, CHUNK_SIZE * total_stripe)
    return s
  
  def Close(self):
    with open(os.path.join(self.path, 'obj_meta.json'), 'w') as f:
      # print(self.meta)
      f.write(json.dumps(self.meta))
  
  def CrashParityNode(self, key, num=1):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    assert(num <= 2)
    if num == 1:
      parity_nodes = obj_meta['parity_nodes']
      node_id = np.random.choice(parity_nodes, size=1)[0]
      self.nodes[node_id].Crash()
      return True
    else:
      parity_nodes = obj_meta['parity_nodes']
      for node_id in parity_nodes:
        self.nodes[node_id].Crash()
      return True
  
  def CrashDataNode(self, key, num=1):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    assert(num <= self.node_num - 2)
    data_nodes = obj_meta['data_nodes']
    node_ids = np.random.choice(data_nodes, size=num, replace=False)
    for node_id in node_ids:
      self.nodes[node_id].Crash()
    return True

  def CorruptDataNode(self, key):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    data_nodes = obj_meta['data_nodes']
    node_id = data_nodes[0]
    self.nodes[node_id].Corrupt(key)
    return True

  def CorruptParityNode(self, key):
    if key not in self.meta['keys']:
      return False
    obj_meta = self.meta['keys'][key]
    parity_nodes = obj_meta['parity_nodes']
    node_id = np.random.choice(parity_nodes, size=1)[0]
    # print('corrupt parity_node: ', node_id, ', nodes: ', parity_nodes)
    self.nodes[node_id].Corrupt(key)
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