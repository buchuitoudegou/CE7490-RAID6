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

  def test_data_node_crash(self,):
    ret = self.store.WriteToStore(self.input_file, "test_data_node_crash")
    self.assertTrue(ret)
    self.store.CrashDataNode("test_data_node_crash", 1)
    ret = self.store.ReadFromStore("test_data_node_crash", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")

    self.store.RecoverAll("test_data_node_crash")
    self.store.CrashDataNode("test_data_node_crash", 2)
    ret = self.store.ReadFromStore("test_data_node_crash", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
    
    self.store.RecoverAll("test_data_node_crash")
    self.store.CrashDataNode("test_data_node_crash", 3)
    ret = self.store.ReadFromStore("test_data_node_crash", self.output_file)
    self.assertTrue(not ret)
  
  def test_parity_node_crash(self):
    ret = self.store.WriteToStore(self.input_file, "test_parity_node_crash")
    self.assertTrue(ret)
    
    self.store.CrashParityNode("test_parity_node_crash", 1)
    ret = self.store.ReadFromStore("test_parity_node_crash", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
    
    self.store.RecoverAll("test_parity_node_crash")
    self.store.CrashParityNode("test_parity_node_crash", 2)
    ret = self.store.ReadFromStore("test_parity_node_crash", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
  
  def test_parity_and_data_node_crash(self):
    ret = self.store.WriteToStore(self.input_file, "test_parity_and_data_node_crash")
    self.assertTrue(ret)
    
    self.store.CrashParityNode("test_parity_and_data_node_crash", 1)
    self.store.CrashDataNode("test_parity_and_data_node_crash", 1)
    ret = self.store.ReadFromStore("test_parity_and_data_node_crash", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
    
  def test_parity_node_corrupt(self):
    ret = self.store.WriteToStore(self.input_file, "test_parity_node_corrupt")
    self.assertTrue(ret)
    
    self.store.CorruptParityNode("test_parity_node_corrupt")
    ret = self.store.ReadFromStore("test_parity_node_corrupt", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")
  
  def test_data_node_corrupt(self):
    ret = self.store.WriteToStore(self.input_file, "test_data_node_corrupt")
    self.assertTrue(ret)
    
    self.store.CorruptDataNode("test_data_node_corrupt")
    ret = self.store.ReadFromStore("test_data_node_corrupt", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    os.system(f"rm {self.output_file}")

  def test_recover_corrupted_data(self):
    ret = self.store.WriteToStore(self.input_file, "test_recover_corrupted_data")
    self.assertTrue(ret)
    
    self.store.CorruptDataNode("test_recover_corrupted_data")
    self.store.meta['keys']['test_recover_corrupted_data']['error'] = 'Data'
    self.store.RecoverCorruptedData()
    ret = self.store.ReadFromStore("test_recover_corrupted_data", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    self.assertEqual(self.store.meta['keys']['test_recover_corrupted_data']['error'], 'No') 
    os.system(f"rm {self.output_file}")

  def test_recover_corrupted_parity(self):
    ret = self.store.WriteToStore(self.input_file, "test_recover_corrupted_parity")
    self.assertTrue(ret)
    
    self.store.CorruptParityNode("test_recover_corrupted_parity")
    self.store.meta['keys']['test_recover_corrupted_parity']['error'] = 'Parity'
    self.store.RecoverCorruptedData()
    ret = self.store.ReadFromStore("test_recover_corrupted_parity", self.output_file)
    self.assertTrue(ret)
    self.assertTrue(filecmp.cmp(self.input_file, self.output_file))
    self.assertEqual(self.store.meta['keys']['test_recover_corrupted_parity']['error'], 'No') 
    os.system(f"rm {self.output_file}")
  
  def tearDown(self):
    super().tearDown()
    self.store.Close()
    

if __name__ == "__main__":
  unittest.main()