"""
In this Python script, a hazelcast client is written to create three clients
that try to increment the value of a variable with key i, but with pessimistic locking,
that is, using the lock/unlock method.
"""

import threading
import time
import hazelcast


def increment_pessimistic():
    """
    this function initializes the variable if there is nothing in the map yet, 
    reads its value by the key i, adds 1 to it, and writes it back synchronizing 
    the clients using lock/unlock.
    """
    function_client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )
    function_map = function_client.get_map("lab2-distributed-map").blocking()

    for _ in range(10000):
        function_map.lock("i")
        value = function_map.get("i")
        value += 1
        function_map.put("i", value)
        # print(value)
        function_map.unlock("i")

    function_client.shutdown()


if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )
    my_map = client.get_map("lab2-distributed-map").blocking()

    my_map.clear()
    my_map.put("i", 0)

    threads = []

    start_time = time.time()

    for i in range(3):
        t = threading.Thread(target=increment_pessimistic)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()

    final_value = my_map.get("i")
    print(f"Final value in map: {final_value}")
    print(f"Execution time: {end_time - start_time} seconds")

    client.shutdown()
