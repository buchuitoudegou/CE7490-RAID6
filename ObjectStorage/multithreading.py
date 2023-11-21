from threading import Thread

def run_tasks(funcs):
  threads = []
  for func in funcs:
    t = Thread(target=func)
    t.start()
    threads.append(t)
  for t in threads:
    t.join()