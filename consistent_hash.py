import bisect
import hashlib
from server_config import NODES

class ConsistentHash:
  def __init__(self,NODES,num_replicas=12):
    self.nodes = NODES
    self.num_replicas = num_replicas
    hash_tuples = [(NODES[j],k,my_hash(str(j)+"_"+str(k))) \
                   for j in range(0,len(self.nodes)) \
                   for k in range(self.num_replicas)]
    hash_tuples.sort(key = lambda x: x[2])
    self.hash_tuples = hash_tuples

  def get_node(self,key):
    h = my_hash(key)
    if h > self.hash_tuples[-1][2]: 
        return self.hash_tuples[0][0]
    hash_values = map(lambda x: x[2],self.hash_tuples)
    index = bisect.bisect_left(list(hash_values),h)
    return self.hash_tuples[index][0]

def my_hash(key):
  return (int(hashlib.md5(key.encode()).hexdigest(),16) % 1000000)/1000000.0

