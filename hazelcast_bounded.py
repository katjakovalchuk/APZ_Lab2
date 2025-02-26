import hazelcast
import time
import threading


def push_back(q, stop):
    for i in range(100):
        if q.size().result() < 10:
            q.put(f"message {i}").result()
            # print("==========================")
            # print("size, ", q.size().result())
            # print("==========================")
            # print(f"Added 'message {i}' to the queue")
        else:
            print(f"Queue is full.")
        time.sleep(0.1)
    stop.set()


def pop_back(q, client_id, read_count, stop):
    while not stop.is_set() or q.size().result() > 0:
        item = q.poll(timeout=1).result()
        if item is not None:
            # print(f"Client {client_id} removed {item} from the queue")
            read_count[client_id] += 1
            time.sleep(0.2)


if __name__ == "__main__":
    client = hazelcast.HazelcastClient(cluster_name="hello-world")
    queue = client.get_queue("wiwiwi")
    queue.clear()
    counter_for_read_messages_per_thread = {1: 0, 2: 0}
    when_to_stop = threading.Event()

    writer_thread = threading.Thread(
        target=push_back,
        args=(
            queue,
            when_to_stop,
        ),
    )

    reader_thread_1 = threading.Thread(
        target=pop_back,
        args=(
            queue,
            1,
            counter_for_read_messages_per_thread,
            when_to_stop,
        ),
    )
    reader_thread_2 = threading.Thread(
        target=pop_back,
        args=(
            queue,
            2,
            counter_for_read_messages_per_thread,
            when_to_stop,
        ),
    )
    writer_thread.start()
    reader_thread_1.start()
    reader_thread_2.start()

    writer_thread.join()
    print("Writer thread finished it tasks.")
    reader_thread_1.join()
    print("Reader thread 1 finished it tasks.")
    reader_thread_2.join()
    print("Reader thread 2 finished it tasks.")

    print(f"Final read counts per thread: {counter_for_read_messages_per_thread}")
    client.shutdown()
