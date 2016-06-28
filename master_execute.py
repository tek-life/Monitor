#!/usr/bin/env python2
import socket
import subprocess
from contextlib import closing
from time import sleep, time
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
s.bind(("0.0.0.0",0))
s.listen(5)
print s.getsockname()[1]
s2,peer=s.accept()
while True:
  monitor_cpu(s2)
  monitor_memory(s2)
  monitor_disk(s2)
  monitor_network(s2)
s2.close()
')"""

#MASTER=""
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
def parse_disk(lines):
  self._filter = re.compile('^\s*(.+):\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*$')
  pass
def parse_network():
  pass


"""
"""
class Monitor(threading.Thread):
  global template
  ssh_lock = threading.Lock()

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
    self.port=0

  def run(self):
    container_s=(self.parse_cpu_container, self.parse_memory_container, self.parse_disk_container, self.parse_network_container)
    parse_func=(parse_cpu, parse_memory,parse_disk,parse_network)
    DEVNULL=open(os.devnull,'rb',0)
    #
    #Lock
    script=template.replace('"',r'\"').replace('\n',r'\n')
    proc=None 
    with Monitor.ssh_lock :
      proc=subprocess.Popen(["ssh", self.host,"python -u -c \"{script}\"".format(script=script)],bufsize=1,stdin=DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)# subprocess is process or thread?
    #End Lock
#    print proc.communicate()
#    print self.host 
#    while True:
#        pass
    with proc.stdout as f:
      self.port=int(f.readline())
      print self.port,
      
    conn=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((self.host,self.port))
  
    with closing(conn.makefile()) as f2:
      while True:
        l=f2.readline()
        if l.startswith(prefix):
          tail = l.lstrip(prefix)
	  cur_time = time() # The cur_time should be move out. Put it over the begin.
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
    #        parse_disk()
    #        print tail
          if tail.startswith("network"):
            id_n = 3
    #        container=parse_network_container
            parse_network()
    #        print tail
        elif l.startswith("END-PIXIU"):
          if id_n ==1:
            temp_tumple=parse_func[id_n](container_s[id_n])
            temp_dict = dict(temp_tumple)
            temp_dict["hostname"] = self.host
	    temp_dict["timestamp"] = cur_time
            self.dict_info.update(temp_dict)
          elif id_n ==0:
            self.dict_info.update(dict([parse_func[id_n](line) for line in container_s[id_n]]))
          container_s[id_n][:]=[]
          print self.dict_info
        else:
          container_s[id_n].append(l)
    
      conn.close()




if __name__=="__main__":
  host_template="10.20.0.{x}"
  monitor_list = []
  for index in [7,9,] :
    host_Monitor = host_template.format(x = str(index))
    monitor_list.append(Monitor(host_Monitor))
    
  for _ in monitor_list:
    _.start()
  for _ in monitor_list:
    _.join()
