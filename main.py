import sys
import os
import logging
from flask import Flask, request, Response
from flask_cors import CORS

from miner import Miner

# Create the application instance
app = Flask(__name__, static_folder='static', instance_path=f'{os.getcwd()}/instance')
CORS(app)
# Set the werkzeug logger to emit only errors
werkzeug_logger = logging.getLogger('werkzeug')
werkzeug_logger.setLevel(logging.ERROR)

# Create a separate logger for your application
logger = logging.getLogger('cordial_miners')  # Custom logger name
logger.setLevel(logging.DEBUG)  # Set to DEBUG level

# Configure the handler and formatter for the app logger
handler = logging.StreamHandler()  # Log to console
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# Add the handler to the app logger
logger.addHandler(handler)

miner = Miner(list(range(5000, 5010)), int(sys.argv[1]))


# Create a URL route in our application for human messages
@app.route('/message', methods=['POST'])
def message_handler():
    message = request.get_json() if request.is_json else None
    logger.info(message)
    miner.receive(message)
    return Response("Success", status=200)


# Create a URL route in our application for DA messages
@app.route('/blocks', methods=['POST'])
def blocks_handler():
    blocks = request.get_json() if request.is_json else None
    logger.info(blocks)
    for block in blocks:
        miner.receive_block(block)
    return Response("Success", status=200)


# If we're running in stand-alone mode, run the application
if __name__ == '__main__':
    port = miner.me
    app.run(host='0.0.0.0', port=port, use_reloader=False)
    print('this is after run')
