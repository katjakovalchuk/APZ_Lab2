"""
In this Python script, a hazelcast client is written to create three
clients that simultaneously increment the value of the same variable without any blocking.
"""
import threading
import hazelcast


def increment(function_map):
    """
    This function initializes the variable if there is nothing in the map
    yet, reads its value at the i key, adds 1 to it, and writes it back.
    """
    function_map.put_if_absent("i", 0)
    for _ in range(10000):
        value = function_map.get("i")
        value += 1
        function_map.put("i", value)


if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )
    my_map = client.get_map("lab2-distributed-map").blocking()

    my_map.clear()

    threads = []
    for client_id in range(3):
        thread = threading.Thread(target=increment, args=(my_map,))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    final_value = my_map.get("i")
    print(f"Final value: {final_value}")


    client.shutdown()
