"""
In this Python script, a hazelcast client is
written to fill three nodes of the cluster with values from 0 to 1000.
"""

import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )
    my_map = client.get_map("lab2-distributed-map").blocking()

    my_map.clear()

    for i in range(1000):
        my_map.put(i, f"Value={i}")
