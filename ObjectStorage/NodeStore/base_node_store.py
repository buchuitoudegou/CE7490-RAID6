class BaseNodeStore:
  def Start(self):
    pass

  def Write(self, key, content):
    pass
  
  def Read(self, key):
    pass
  
  def Alive(self):
    return True
  
  def Crash(self):
    pass
  
  def Recover(self):
    pass
  
  def Corrupt(self, key):
    pass
  
  def Close(self):
    pass