import zmq

context = zmq.Context()
socket = context.socket(zmq.PULL)
socket.bind("tcp://127.0.0.1:5555")

print("ZMQ Logger started. Waiting for messages...\n")

while True:
    msg = socket.recv_json()
    print("[ZMQ]", msg)
