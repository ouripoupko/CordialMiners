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
    requests.post(servers[i],
                  json=f'message {counter}')
    counter = counter+1
    input()

