import timeit
import os
from ObjectStorage import ObjectStore



def test_func(test, test_time=10):  
  os.system("rm -rf /tmp/raid6/*")
  store = ObjectStore("/tmp/raid6", 5)
  t = timeit.Timer(test(store))
  print('write and read (5): ', t.timeit(test_time))
  
  os.system("rm -rf /tmp/raid6/*")
  store = ObjectStore("/tmp/raid6", 7)
  t = timeit.Timer(test(store))
  print('write and read (7): ', t.timeit(test_time))
  
  os.system("rm -rf /tmp/raid6/*")
  store = ObjectStore("/tmp/raid6", 9)
  t = timeit.Timer(test(store))
  print('write and read (9): ', t.timeit(test_time))
  
  os.system("rm -rf /tmp/raid6/*")
  store = ObjectStore("/tmp/raid6", 11)
  t = timeit.Timer(test(store))
  print('write and read (11): ', t.timeit(test_time))

def test_normal_read_write(store):
  input_file = "example/test.log"
  output_file = "output.txt"
  def ret():
    ret = store.WriteToStore(input_file, "test_normal")
    assert(ret)
    ret = store.ReadFromStore("test_normal", output_file)
    assert(ret)
  return ret

def test_normal_read_write2(store):
  input_file = "example/Tank.fbx"
  output_file = "output.txt"
  def ret():
    ret = store.WriteToStore(input_file, "test_normal")
    assert(ret)
    ret = store.ReadFromStore("test_normal", output_file)
    assert(ret)
  return ret

def test_crash_read_write(pstore):
  input_file = "example/test.log"
  output_file = "output.txt"
  def ret():
    os.system("rm -rf /tmp/raid6/*")
    store = ObjectStore("/tmp/raid6", pstore.node_num)
    ret = store.WriteToStore(input_file, "test_crash")
    assert(ret)
    store.CrashDataNode("test_crash", 1)
    ret = store.ReadFromStore("test_crash", output_file)
    assert(ret)
  return ret

def test_crash_read_write2(pstore):
  input_file = "example/Tank.fbx"
  output_file = "output.txt"
  def ret():
    os.system("rm -rf /tmp/raid6/*")
    store = ObjectStore("/tmp/raid6", pstore.node_num)
    ret = store.WriteToStore(input_file, "test_crash")
    assert(ret)
    store.CrashDataNode("test_crash", 1)
    ret = store.ReadFromStore("test_crash", output_file)
    assert(ret)
  return ret

print('normal read write')
test_func(test_normal_read_write, 100)
print('data rebuild')
test_func(test_crash_read_write, 20)
print('large data read and write')
test_func(test_normal_read_write2, 3)
print('large file data rebuild')
test_func(test_crash_read_write2, 3)