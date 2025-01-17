import requests
import threading
from time import sleep
import random

servers = [f'http://localhost:{index}/message' for index in range(5000, 5010)]
print(servers)

def bombard_away(my_server):
    for index in range(100):
        print(f'sending message {index} to agent {my_server}')
        reply = requests.post(my_server, json=f'agent {my_server} message {index}')
        print(reply.text)
        if reply.status_code != 200:
            input()
        sleep(0.5+index/10)

# Create a thread and target the function
for server in servers:
    thread = threading.Thread(target=bombard_away, args=(server,))

    # Start the thread
    thread.start()

