#!/usr/bin/env python2
import socket
import subprocess
from contextlib import closing
from time import sleep
import os
from collections import namedtuple
import threading

template=r"""exec('
import socket,time
socket.setdefaulttimeout(10)
END="END-PIXIU"
def monitor_cpu(conn):
  conn.send("PIXIU-cpu"+chr(10))    
  with open("/proc/stat") as f:
    conn.send("".join([x for x in f.readlines() if x.startswith("cpu")]))
  conn.send(END+chr(10))
  time.sleep(1)
def monitor_memory(conn):
  conn.send("PIXIU-memory"+chr(10))
  with open("/proc/meminfo") as f:
    mem = dict([(a, b.split()[0].strip()) for a, b in [x.split(":") for x in f.readlines()]])
    conn.send(":".join([mem[field] for field in ["MemTotal", "Buffers", "Cached", "MemFree", "Mapped"]])+chr(10))
  conn.send(END+chr(10))
  time.sleep(1)
def monitor_disk(conn):
  conn.send("PIXIU-disk"+chr(10))
  with open("/proc/diskstats") as f:
    conn.send("".join([x for x in f.readlines() if x.split()[2].startswith("sda")]))
  conn.send(END+chr(10))
def monitor_network(conn):
  conn.send("PIXIU-network"+chr(10))
  with open("/proc/net/dev") as f:
    conn.send("".join([x for x in f.readlines() if x.split()[0].startswith("eth")]))
  conn.send(END+chr(10))

s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1",9000))
s.listen(5)
s2,peer=s.accept()
while True:
  monitor_cpu(s2)
  monitor_memory(s2)
  monitor_disk(s2)
  monitor_network(s2)
s2.close()
')"""

#MASTER=""
MONITOR_SITE="127.0.0.1" #just for test
prefix="PIXIU-"

"""
namedtuple
"""

cpu_namedtuple=namedtuple("CPU",["label", "user", "nice", "system", "idle", "iowait", "irq", "softirq"])
memory_namedtuple=namedtuple("Memory",["label", "total", "used", "buffer_cache", "free", "map_"])
#disk_nametuple=namedtuple()
#network_nametuple=namedtuple()

"""
 parse the recive data
"""
def parse_cpu(line):
  fields = line.split()
  return (fields[0], cpu_namedtuple(fields[0], *[int(x) for x in fields[1:8]]))

def parse_memory(list_line):
  total, buffers, cached, free, mapped= [int(x) for x in list_line[0].split(":")]
  return [("Memory", memory_namedtuple("total", total=total, used=buffers, buffer_cache=cached, free=free, map_=mapped ))]
  pass
def parse_disk():
  pass
def parse_network():
  pass


"""
"""
class Monitor(threading.Thread):
  global template

  def __init__(self, host):
    super(Monitor, self).__init__()
    self.host=host #Target
    """
     DataStructure for Parse
    """
    self.parse_cpu_container=[]
    self.parse_memory_container=[]
    self.parse_disk_container=[]
    self.parse_network_container=[]

    self.dict_info = {}

  def run(self):
    container_s=(self.parse_cpu_container, self.parse_memory_container, self.parse_disk_container, self.parse_network_container)
    parse_func=(parse_cpu, parse_memory,parse_disk,parse_network)
    DEVNULL=open(os.devnull,'rb',0)
    #
    #Lock
    script=template.replace('"',r'\"').replace('\n',r'\n')
    proc=subprocess.Popen(["ssh", self.host,"python -u -c \"{script}\"".format(script=script)],bufsize=1,stdin=DEVNULL,stderr=subprocess.STDOUT)# subprocess is process or thread?
    #End Lock
    
    sleep(3)
    conn=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((self.host,9000))
  
    with closing(conn.makefile()) as f2:
      while True:
        l=f2.readline()
        if l.startswith(prefix):
          tail = l.lstrip(prefix)
          if tail.startswith("cpu"):
            id_n = 0
    #        parse_cpu()
    #        print tail
          if tail.startswith("memory"):
            id_n = 1
    #        container=parse_memory_container
    #        parse_memory()
    #        print tail
          if tail.startswith("disk"):
            id_n = 2
    #        container=parse_disk_container
            parse_disk()
    #        print tail
          if tail.startswith("network"):
            id_n = 3
    #        container=parse_network_container
            parse_network()
    #        print tail
        elif l.startswith("END-PIXIU"):
          if id_n ==1:
            temp_tumple=parse_func[id_n](container_s[id_n])
            print dict(temp_tumple)
            self.dict_info.update(dict(temp_tumple))
          elif id_n ==0:
            self.dict_info.update(dict([parse_func[id_n](line) for line in container_s[id_n]]))
          container_s[id_n][:]=[]
          print self.dict_info
        else:
          container_s[id_n].append(l)
    
      conn.close()
      pass




if __name__=="__main__":
  monitor=Monitor("127.0.0.1")
  monitor.start()
  monitor.join()