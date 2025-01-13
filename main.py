import sys
import os
import logging
from flask import Flask, request
from flask_cors import CORS
import hashlib
from datetime import datetime

from miner import Miner

# Create the application instance
app = Flask(__name__, static_folder='static', instance_path=f'{os.getcwd()}/instance')
CORS(app)
logger = app.logger

miner = Miner(list(range(5001, 5011)), int(sys.argv[1]))


# Create a URL route in our application for human messages
@app.route('/message', methods=['POST'])
def message_handler():
    message = request.get_json() if request.is_json else None
    logger.info(message)
    block = {'creator': miner.me,
             'timestamp': datetime.now().strftime('%Y%m%d%H%M%S%f'),
             'payload': message,
             'pointers': []}
    block['hash_code'] = hashlib.sha256(str(block).encode('utf-8')).hexdigest()
    return miner.receive(block)


# Create a URL route in our application for DA messages
@app.route('/blocks', methods=['POST'])
def blocks_handler():
    blocks = request.get_json() if request.is_json else None
    logger.info(blocks)
    for block in blocks:
        miner.receive(block)


# If we're running in stand-alone mode, run the application
if __name__ == '__main__':
    port = miner.me
    conf_kwargs = {'format': '%(asctime)s %(levelname)-8s %(message)s',
                   'datefmt': '%Y-%m-%d %H:%M:%S'}
    logging.basicConfig(**conf_kwargs)

    logger = logging.getLogger('werkzeug')
    logger.setLevel(logging.DEBUG)
    app.run(host='0.0.0.0', port=port, use_reloader=False)
    print('this is after run')
