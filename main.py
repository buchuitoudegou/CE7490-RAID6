import ObjectStorage
import os
import filecmp
import unittest


class TestObjectStore(unittest.TestCase):
  def setUp(self):
    super().setUp()
    os.system("rm -rf /tmp/raid6/*")
    self.store = ObjectStorage.ObjectStore("/tmp/raid6/", 5)
    self.input_file = "example/test.log"
    self.output_file = "output.txt"
  
  def test_normal_write(self):
    ret = self.store.WriteToStore(self.input_file, "test_normal")
    assert(ret)
    ret = self.store.ReadFromStore("test_normal", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))

  def test_data_node_corruption(self,):
    ret = self.store.WriteToStore(self.input_file, "test_data_node_corruption")
    self.assertTrue(ret)
    self.store.CorruptDataNode("test_data_node_corruption", 1)
    ret = self.store.ReadFromStore("test_data_node_corruption", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")

    self.store.RecoverAll("test_data_node_corruption")
    self.store.CorruptDataNode("test_data_node_corruption", 2)
    ret = self.store.ReadFromStore("test_data_node_corruption", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
    
    self.store.RecoverAll("test_data_node_corruption")
    self.store.CorruptDataNode("test_data_node_corruption", 3)
    ret = self.store.ReadFromStore("test_data_node_corruption", self.output_file)
    self.assertTrue(not ret)
  
  def test_parity_node_corruption(self):
    ret = self.store.WriteToStore(self.input_file, "test_parity_node_corruption")
    self.assertTrue(ret)
    
    self.store.CorruptParityNode("test_parity_node_corruption", 1)
    ret = self.store.ReadFromStore("test_parity_node_corruption", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
    
    self.store.RecoverAll("test_parity_node_corruption")
    self.store.CorruptParityNode("test_parity_node_corruption", 2)
    ret = self.store.ReadFromStore("test_parity_node_corruption", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")

  def tearDown(self):
    super().tearDown()
    self.store.Close()
    

if __name__ == "__main__":
  unittest.main()