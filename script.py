import requests
from time import sleep
import random

servers = [f'http://localhost:{index}/message' for index in range(5000, 5010)]
print(servers)

# for i in range(len(servers)):
#     requests.post(servers[i],
#                   json=f'message {i}')

counter = 0
while True:
    i = random.randint(0, 9)
    print(f'sending message {counter} to agent {i}')
    reply = requests.post(servers[i], json=f'message {counter}')
    print(reply.text)
    if reply.status_code != 200:
        input()
    counter = counter+1
    sleep(0.5)

