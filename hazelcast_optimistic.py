"""
In this Python script, a hazelcast client is written to create three clients that try
to increment the value of a variable with key i, but with optimistic locking, 
that is, by reading the variable, adding 1 to it, checking if any thread has added anything yet, 
and if not, writing the new value to the variable.
"""

import threading
import time
import hazelcast


def increment_optimistic():
    """
    this function reads variable with key=i,
    adds 1 to it, checks if any thread has added anything yet,
    and if not, writes a new value to the variable.

    replace_if_same:
    key – The specified key.
    old_value – Replace the key value if it is the old value.
    new_value – The new value to replace the old value.
    """

    function_client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )
    function_map = function_client.get_map("lab2-distributed-map").blocking()

    for _ in range(10000):
        retry = True
        while retry:
            old_value = function_map.get("i")
            new_value = old_value + 1
            if function_map.replace_if_same("i", old_value, new_value):
                # print(new_value)
                retry = False
            time.sleep(0.01)

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
        t = threading.Thread(target=increment_optimistic)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    end_time = time.time()

    final_value = my_map.get("i")
    print(f"Final value in map: {final_value}")
    print(f"Execution time: {end_time - start_time} seconds")

    client.shutdown()
