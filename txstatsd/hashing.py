import bisect

from hashlib import md5


class ConsistentHashRing:

  def __init__(self, nodes):
      self.ring = []
      for node in nodes:
          self.add_node(node)

  def compute_ring_position(self, key):
      big_hash = md5(str(key)).hexdigest()
      small_hash = int(big_hash[:4], 16)
      return small_hash

  def add_node(self, node):
      position = self.compute_ring_position(str(node))
      entry = (position, node)
      bisect.insort(self.ring, entry)

  def remove_node(self, node):
      self.ring = [entry for entry in self.ring
                   if str(entry[1]) != str(node)]

  def get_node(self, key):
      assert self.ring
      position = self.compute_ring_position(key)
      search_entry = (position, None)
      index = bisect.bisect_left(self.ring, search_entry) % len(self.ring)
      entry = self.ring[index]
      return entry[1]
