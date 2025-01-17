import sys
import os
import logging
from flask import Flask, request, Response
from flask_cors import CORS
import threading
import queue
from time import sleep

from miner import Miner

# Create the application instance
app = Flask(__name__, static_folder='static', instance_path=f'{os.getcwd()}/instance')
CORS(app)
# Set the werkzeug logger to emit only errors
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# Create a separate logger for your application
logger = logging.getLogger('cordial_miners')  # Custom logger name
logger.setLevel(logging.INFO)  # Set to DEBUG level

# Configure the handler and formatter for the app logger
handler = logging.StreamHandler()  # Log to console
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the app logger
logger.addHandler(handler)

miner = Miner(list(range(5000, 5010)), int(sys.argv[1]))
message_queue = queue.Queue()
block_queue = queue.Queue()

def queue_reader():
    counter = 0
    while True:
        both_empty = True
        counter = 0
        while miner.messages and not block_queue.empty():
            both_empty = False
            miner.receive_block(block_queue.get())
            block_queue.task_done()
        if not message_queue.empty():
            both_empty = False
            miner.receive(message_queue.get())
            message_queue.task_done()
        if not block_queue.empty():
            both_empty = False
            miner.receive_block(block_queue.get())
            block_queue.task_done()
        if both_empty:
            if counter == 10:
                counter = 0
                if len(miner.blocklace) > len(miner.outputBlocks):
                    miner.receive(None)
            else:
                counter += 1
                sleep(0.1)
        else:
            counter = 0
        logger.debug(f'number of messages {message_queue.qsize()} number of blocks {block_queue.qsize()}')

reader_thread = threading.Thread(target=queue_reader, daemon=True)
reader_thread.start()

# Create a URL route in our application for human messages
@app.route('/message', methods=['POST'])
def message_handler():
    message = request.get_json() if request.is_json else None
    message_queue.put(message)
    return Response(f'miner {miner.me} round {miner.round} blocks {len(miner.blocklace)} output {len(miner.outputBlocks)} equivocators {miner.equivocators}', status=200)


# Create a URL route in our application for DA messages
@app.route('/blocks', methods=['POST'])
def blocks_handler():
    blocks = request.get_json() if request.is_json else None
    for block in blocks:
        block_queue.put(block)
    return Response("Success", status=200)


# If we're running in stand-alone mode, run the application
if __name__ == '__main__':
    port = miner.me
    app.run(host='0.0.0.0', port=port, use_reloader=False)
    print('this is after run')
