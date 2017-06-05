"""
You must use hashlib to hash the node ('0.0.0.0:3000') and key ('mykey') combination.
Example: 
node = '0.0.0.0:3000' + 'my-key'
x = node + key
hash = hashlib.md5(x).hexdigest()

More@https://docs.python.org/2/library/hashlib.html
"""
# TODO: Add any required import
import hashlib
from client import DBClient

md5_constructor = hashlib.md5

class RendezvousHash(object):
    """
    This class implements the Rendezvous (HRW) hashing logic.
    DO NOT USE ANY STATIC CLASS VARIABLES!
    """

    def __init__(self, nodes=None):
        """
        Initialize an instance with a node list and others.
        A node means a server host name and its listening port. E.g. '0.0.0.0:3000' 
        :param nodes: a list of DB server nodes to register.
        """
        # TODO
        self.nodes = nodes

    def weight(self, node, key):
        return None

    def _hash_digest(self, key):
        m = md5_constructor()
        m.update(key)
        return map(ord, m.digest())

    
    def get_node(self, key):
        """
        Find the highest hash value via hash(node+key) and the node that generates the highest
        value among all nodes.
        :param key: a string key name.
        :return the highest node.
        """
        highest_node = None

        weights = []
        index = 0
        temp_weight = 0
        count = 0
        for node in self.nodes:
            #n = self._hash_digest(node)
            #n = int(hashlib.sha1(node).hexdigest(), 16) % (10 ** 8)
            n = long(hashlib.md5(node).hexdigest(), 16)
            #k = int(hashlib.sha1(key).hexdigest(), 16) % (10 ** 8)
            k = long(hashlib.md5(key).hexdigest(), 16)
            w = n + k
            weights.append((w, node))
            if w > temp_weight:
                temp_weight = w
                index = count
            
            count += 1


        highest_node = self.nodes[index]
        
        return highest_node

#a = RendezvousHash(['a', 'b', 'c', 'd'])

#print a.get_node('abc')


class RendezvousHashDBClient(RendezvousHash):
    """
    This class extends from the above RendezvousHash class and
    integrates DBClient (see@client.py) with RendezvousHash so that 
    client can PUT and GET to the DB servers while the rendezvous hash shards 
    the data across multiple DB servers.
    DO NOT USE ANY STATIC CLASS VARIABLES!
    """

    def __init__(self, db_servers=None):
        """
        1. Initialize the super/parent RendezvousHash class.
        Class inheritance@http://www.python-course.eu/python3_inheritance.php
        2. Create DBClient instance for all servers and save them in a dictionary.
        :param db_servers: a list of DB servers: ['0.0.0.0:3000', '0.0.0.0:3001', '0.0.0.0:3002']
        """
        # TODO

        self.r_hash = RendezvousHash(db_servers)
        self.client_map = {}

        for server in db_servers:
            host_port = server.split(':')
            host = host_port[0]
            port = int(host_port[1])

            self.client_map[server] = DBClient(host, port)





        
 

    def put(self, key, value):
        """
        1. Get the highest Rendezvous node for the given key.
        2. Retrieve the DBClient instance reference by the node.
        3. Save the value into DB via client's put(). 
        :param key: a string key.
        :param value: a string key-value pair dictionary to be stored in DB. 
        :return a PutResponse - see@db.proto
        NOTE: Both key and value must be the string type.
        """
        # TODO

        high_node = self.r_hash.get_node(key)

        host_port = high_node.split(':')
        host = host_port[0]
        port = int(host_port[1])
        db_inst = DBClient(host, port)

        #out_key = 'foobar-1@gmail.com'

        put_res = db_inst.put(key, value)

        return put_res
        #return 'Put Response'

    
    def get(self, key):
        """
        1. Get the highest Rendezvous node for the given key.
        2. Retrieve the DBClient instance reference by the node.
        3. Get the value by the key via client's get(). 
        :param key: a string key.
        :param value: a string key-value pair dictionary to be stored in DB. 
        :return a GetResposne - see@db.proto
        """
        # TODO

        high_node = self.r_hash.get_node(key)

        host_port = high_node.split(':')
        host = host_port[0]
        port = int(host_port[1])
        db_inst = DBClient(host, port)

        get_res = db_inst.get(key)

        return get_res
        #return 'GetResponse - data retrieved from DB'


    def info(self):
        """
        Return a list of InfoResponse from all servers.
        1. Invoke DB client's info() to retrieve server info for all servers.
        """
        server_info = []

        for url,client in self.client_map.items():
            server_info.append(client.info())
        # TODO
        return server_info

        