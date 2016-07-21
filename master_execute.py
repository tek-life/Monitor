#!/usr/bin/env python2
import socket
import subprocess
from contextlib import closing
from time import sleep, time
import os
from collections import namedtuple
import threading
import re

template=r"""exec('
import socket,time
socket.setdefaulttimeout(10)
END="END-PIXIU"
def monitor_cpu(conn):
  conn.send("PIXIU-cpu"+chr(10))    
  with open("/proc/stat") as f:
    conn.send("".join([x for x in f.readlines() if x.startswith("cpu")]))
  conn.send(END+chr(10))
  time.sleep(5)
def monitor_memory(conn):
  conn.send("PIXIU-memory"+chr(10))
  with open("/proc/meminfo") as f:
    mem = dict([(a, b.split()[0].strip()) for a, b in [x.split(":") for x in f.readlines()]])
    conn.send(":".join([mem[field] for field in ["MemTotal", "Buffers", "Cached", "MemFree", "Mapped"]])+chr(10))
  conn.send(END+chr(10))
  time.sleep(5)
def monitor_disk(conn):
  conn.send("PIXIU-disk"+chr(10))
  with open("/proc/diskstats") as f:
    conn.send("".join([x for x in f.readlines() if not x.split()[2].startswith("loop") and not x.split()[2].startswith("ram") and x.split()[3]!="0"]))
  conn.send(END+chr(10))
  time.sleep(5)
def monitor_network(conn):
  conn.send("PIXIU-network"+chr(10))
  with open("/proc/net/dev") as f:
    conn.send("".join([x for x in f.readlines() if not x.split()[0].startswith("lo")]))
  conn.send(END+chr(10))
  time.sleep(5)

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
  s2.send("END------"+chr(10))
s2.close()
')"""

#MASTER=""
prefix="PIXIU-"

"""
namedtuple
"""

cpu_namedtuple=namedtuple("CPU",["label", "user", "nice", "system", "idle", "iowait", "irq", "softirq"])
memory_namedtuple=namedtuple("Memory",["label", "total", "used", "buffer_cache", "free", "map_"])
disk_namedtuple=namedtuple("Disk", ["label", "io_read", "bytes_read", "time_spend_read", "io_write", "bytes_write", "time_spend_write"])
network_namedtuple=namedtuple("Network", ['label', "recv_bytes", "recv_packets", "recv_errs", "recv_drop",
                                "send_bytes", "send_packets", "send_errs", "send_drop"])

class CPU(cpu_namedtuple):
  pass
class Memory(memory_namedtuple):
  pass
class Disk(disk_namedtuple):
  pass
class Network(network_namedtuple):
  pass

network_filter = re.compile('^\s*(.+):\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+\d+\s+\d+\s+\d+\s+\d+\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+).*$')
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
  def __parse_disk(_line):
    line = _line.split()
    return (line[2],disk_namedtuple(line[2], io_read=int(line[3]), bytes_read=int(line[5])*512, time_spend_read=int(line[6])/1000.0,
    io_write=int(line[7]), bytes_write=int(line[9])*512, time_spend_write=int(line[10])/1000.0))
  
  return dict([__parse_disk(line) for line in lines])

def parse_network(line):
  matched = network_filter.match(line)
  if matched:
    lists=matched.groups()
    return (lists[0], network_namedtuple(lists[0],*[int(x) for x in lists[1:]]))


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
    
    with proc.stdout as f:
      self.port=int(f.readline())
      print self.port,
      
    conn=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    conn.connect((self.host,self.port))
  
    with closing(conn.makefile()) as f:
      while True:
        l=f.readline()
        if l.startswith(prefix):
          tail = l.lstrip(prefix)
	  cur_time = time() # The cur_time should be move out. Put it over the begin.
          if tail.startswith("cpu"):
            id_n = 0
          if tail.startswith("memory"):
            id_n = 1
          if tail.startswith("disk"):
            id_n = 2
          if tail.startswith("network"):
            id_n = 3
        elif l.startswith("END-PIXIU"):
          if id_n ==1:
            temp_tumple=parse_func[id_n](container_s[id_n])
            temp_dict = dict(temp_tumple)
            temp_dict["hostname"] = self.host
	    temp_dict["timestamp"] = cur_time
            self.dict_info.update(temp_dict)
          elif id_n ==0:
            self.dict_info.update(dict([parse_func[id_n](line) for line in container_s[id_n]]))
          elif id_n == 2:
            self.dict_info.update(parse_func[id_n](container_s[id_n]))
          elif id_n == 3:
            self.dict_info.update(dict(filter(lambda x:x,[parse_func[id_n](line) for line in container_s[id_n]])))
        elif l.startswith("END------"):   
          with open("/root/log.log",'a+') as f2:
            f2.write(repr(self.dict_info)+"\n")
          container_s[id_n][:]=[]
          self.dict_info.clear()

        else:
          container_s[id_n].append(l)
    
      conn.close()


def generate_report(log_path, output_path):
  with open(log_path) as f:
    datas=[eval(x) for x in f.readlines()]
  print datas

if __name__=="__main__":
  host_template="10.20.0.{x}"
  monitor_list = []
  
  generate_report("/root/log.log","/root/log.html")
"""
  for index in [7,9,] :
    host_Monitor = host_template.format(x = str(index))
    monitor_list.append(Monitor(host_Monitor))
    
  for _ in monitor_list:
    _.start()
  for _ in monitor_list:
    _.join()
"""
