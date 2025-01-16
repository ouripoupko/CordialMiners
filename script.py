import requests
from time import sleep
import random

servers = [f'http://localhost:{index}/message' for index in range(5000, 5010)]
print(servers)

# for i in range(len(servers)):
#     requests.post(servers[i],
#                   json=f'message {i}')

for round in range(1000):
    i = random.randint(0, 9)
    print(f'sending message {round} to agent {i}')
    reply = requests.post(servers[i], json=f'message {round}')
    print(reply.text)
    if reply.status_code != 200:
        input()

