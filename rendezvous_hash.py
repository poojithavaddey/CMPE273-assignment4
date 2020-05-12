from collections import defaultdict
import mmh3
from server_config import NODES

class RendezvousHash(object):

    def __init__(self, nodes=None, seed=0):
        self.nodes = []
        self.seed = seed
        if nodes is not None:
            self.nodes = nodes
        self.hash_function = lambda x: mmh3.hash(x, seed)

    def add_node(self, node):
        if node not in self.nodes:
            self.nodes.append(node)

    def remove_node(self, node):
        if node in self.nodes:
            self.nodes.remove(node)
        else:
            raise ValueError("No such node %s to remove" % (node))

    def get_node(self, key):
        high_score = -1
        highest_node = None
        for node in self.nodes:
            score = self.hash_function("%s%s" % (str(node), str(key)))
            if score > high_score:
                (high_score, highest_node) = (score, node)
        return highest_node
