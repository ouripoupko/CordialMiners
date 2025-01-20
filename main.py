import sys
import os
import logging
from flask import Flask, request, Response
from flask_cors import CORS
import threading
import queue
from time import sleep

from miner import Miner, DEPTH, PAYLOAD

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
block_queue = queue.PriorityQueue()

class OrderedBlock:
    def __init__(self, block, order):
        self.block = block
        self.order = order

    def __lt__(self, other):
        return self.order < other.order

def queue_reader():
    counter = 0
    while True:
        ordered_block = None if block_queue.empty() else block_queue.get()
        message_exists = not message_queue.empty()
        prioritise_block = ordered_block is not None and ordered_block.order <= miner.round
        if message_exists and not prioritise_block:
            miner.receive(message_queue.get())
            message_queue.task_done()
        if ordered_block:
            if prioritise_block or not message_exists:
                miner.receive_block(ordered_block.block)
            else:
                block_queue.put(ordered_block)
        if message_exists or ordered_block is not None:
            counter = 0
        else:
            if counter == 10:
                waiting_blocks = [key for key in miner.blocklace
                                  if key not in miner.outputBlocks and miner.blocklace[key][PAYLOAD]]
                logger.info(f'{len(waiting_blocks)} blocks waiting')
                if waiting_blocks:
                    counter = 0
                    miner.receive(None)
                else:
                    counter += 1
            else:
                counter += 1
                sleep(0.1)
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
        if DEPTH in block:
            block_queue.put(OrderedBlock(block, block[DEPTH]))
    return Response("Success", status=200)


# If we're running in stand-alone mode, run the application
if __name__ == '__main__':
    port = miner.me
    app.run(host='0.0.0.0', port=port, use_reloader=False)
    print('this is after run')
