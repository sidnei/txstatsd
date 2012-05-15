"""
A copy of carbon.hashing, with higher replica count and more bits from the big
hash, for a more even distribution.

This copy is included here so that txstatsd.client doesn't depend on carbon.
"""

import bisect

from hashlib import md5


class ConsistentHashRing:

    def __init__(self, nodes, replica_count=1024):
        self.ring = []
        self.nodes = set()
        self.replica_count = replica_count
        for node in nodes:
            self.add_node(node)

    def compute_ring_position(self, key):
        big_hash = md5(str(key)).hexdigest()
        small_hash = long(big_hash[:8], 16)
        return small_hash

    def add_node(self, node):
        self.nodes.add(node)
        for i in range(self.replica_count):
            replica_key = "%s:%d" % (node, i)
            position = self.compute_ring_position(replica_key)
            entry = (position, node)
            bisect.insort(self.ring, entry)

    def remove_node(self, node):
        self.nodes.discard(node)
        self.ring = [entry for entry in self.ring if entry[1] != node]

    def get_node(self, key):
        assert self.ring
        position = self.compute_ring_position(key)
        search_entry = (position, None)
        index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
        entry = self.ring[index]
        return entry[1]

    def get_nodes(self, key):
        nodes = []
        position = self.compute_ring_position(key)
        search_entry = (position, None)
        index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
        last_index = (index - 1) % len(self.ring)
        while len(nodes) < len(self.nodes) and index != last_index:
            next_entry = self.ring[index]
            (position, next_node) = next_entry
            if next_node not in nodes:
                nodes.append(next_node)
            index = (index + 1) % len(self.ring)
        return nodes
