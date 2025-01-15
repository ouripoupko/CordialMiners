import requests
import hashlib
from datetime import datetime
import logging

PAYLOAD = 'payload'
CREATOR = 'creator'
TIMESTAMP = 'timestamp'
POINTERS = 'pointers'
DEPTH = 'depth'
HASHCODE = 'hash_code'

logger = logging.getLogger('cordial_miners')

# assumptions (forced by the logic of adding blocks to the blocklace):
# 1- every block points to all tips. A fast miner will continue to point to the same tips of slower miners.
# 2- every block points to the previous block of the same miner.
# 3- 'tips' holds the tips of all good miners.
# 4- 'equivocators' holds all visible tips of all visible equivocators.
# 5- every tip holds a chain of its creator all the way to depth 0.

class Miner:

    def __init__(self, everyone, me):
        self.everyone = sorted(everyone)
        self.others = [other for other in self.everyone if other != me]
        n = len(self.everyone)
        f = (n - 1) // 3
        self.super_majority = (n + f) // 2
        self.me = me
        self.round = -1
        self.round_collection = {}
        self.next_non_final_round = 0
        self.max_depth = 0
        self.buffer = {}
        self.blocklace = {}
        self.tips = {}
        self.equivocators = {}
        self.wavelength = 4
        self.leader = self.es_leader
        self.completed_round = self.es_completed_round
        self.previous_final_leader = None
        self.outputBlocks = []
        self.messages = []

    # auxiliary functions
    def leaf_of_creator(self, head):
        creator = self.blocklace[head][CREATOR]
        child = head
        reply = head
        while child:
            reply = child
            child = next((kid for kid in self.blocklace[reply][POINTERS] if self.blocklace[kid][CREATOR] == creator), None)
        return reply

    # Algorithm 1
    def create_block(self, message):
        block = {PAYLOAD: message,
                 CREATOR: self.me,
                 TIMESTAMP: datetime.now().strftime('%Y%m%d%H%M%S%f'),
                 # TODO: check if can use tips directly
                 POINTERS: [key for key in self.blocklace if self.blocklace[key][DEPTH] == self.round-1],
                 DEPTH: self.round}
        block['hash_code'] = hashlib.sha256(str(block).encode('utf-8')).hexdigest()
        return block

    def observes(self, observer, observed):
        children = {observer}
        while children:
            if observed in children:
                return True
            children = {grand_child for child in children for grand_child in self.blocklace[child][POINTERS]}
        return False

    # TODO: make sure we don't use this horrible function
    def closure(self, head):
        if not head:
            return set()
        block = self.blocklace[head]
        children = set(block[POINTERS])
        reply = {head}
        while children:
            reply.update(children)
            children = {grand_child for child in children for grand_child in self.blocklace[child][POINTERS]}
        return reply

    # TODO: make sure we don't use this horrible function
    def equivocation(self, key1, key2):
        c1 = self.blocklace[key1][CREATOR] == self.blocklace[key2][CREATOR]
        c2 = self.observes(key1, key2)
        c3 = self.observes(key2, key1)
        return c1 and not c2 and not c3

    def equivocator(self, _q, _b):
        # I trust that there are no equivocators, because I check them when adding a new block to the blocklace
        return self is None

    def correct_block(self, block):
        a = HASHCODE in block and isinstance(block[HASHCODE], str)
        b = CREATOR in block and block[CREATOR] in self.everyone
        c = POINTERS in block and isinstance(block[POINTERS], list)
        d = DEPTH in block and isinstance(block[DEPTH], int)
        e = c and d and block[DEPTH] == 0 and len(block[POINTERS]) == 0
        f = d and block[DEPTH] > 0
        g = c and f and len(block[POINTERS]) > self.super_majority
        reply = a and b and c and d and (e or g)
        return reply

    # TODO: check - should a block approve itself?
    def approves(self, head, key):
        children = set(self.blocklace[head][POINTERS])
        creator = self.blocklace[key][CREATOR]
        # instead of checking every block in the closure of 'head', if it equivocates 'block'
        # check all tips of the equivocator. if a tip observes 'block' it is safe
        # if it doesn't, enough to check if the leaf of that tip is in the closure of 'head'
        equivocating_blocks = set()
        if creator in self.equivocators:
            equivocating_blocks = {self.leaf_of_creator(tip)
                                   for tip in self.equivocators[creator]
                                   if not self.observes(tip, key)}
        in_tree = False
        equivocate = False
        while children and not equivocate:
            equivocate = not children.isdisjoint(equivocating_blocks)
            if key in children:
                in_tree = True
            children = {grand_child for child in children for grand_child in self.blocklace[child][POINTERS]}
        return in_tree and not equivocate

    def ratifies(self, head, key):
        depth = self.blocklace[key][DEPTH]
        approvers = set()
        observers = {head}
        while observers:
            observer = observers.pop()
            if self.approves(observer, key):
                approvers.add(self.blocklace[observer][CREATOR])
            observers.update({child
                              for child in self.blocklace[observer][POINTERS]
                              if self.blocklace[child][DEPTH] >= depth})
        return len(approvers) > self.super_majority

    def super_ratifies(self, heads, key):
        depth = self.blocklace[key][DEPTH]
        ratifiers = set()
        observers = set(heads)
        while observers:
            observer = observers.pop()
            if self.ratifies(observer, key):
                ratifiers.add(self.blocklace[observer][CREATOR])
                observers.update({child
                                  for child in self.blocklace[observer][POINTERS]
                                  if self.blocklace[child][DEPTH] >= depth})
        return len(ratifiers) > self.super_majority

    def blocklace_prefix(self, min_depth, max_depth):
        return {block for block in self.blocklace if min_depth < self.blocklace[block][DEPTH] <= max_depth}

    def cordial_round(self, cycle):
        creators = {block[CREATOR] for block in self.blocklace.values() if block[DEPTH] == cycle}
        logger.debug(f'round {cycle} has {len(creators)} creators')
        return len(creators) > self.super_majority

    # Algorithm 2
    def tau(self):
        final_leader = self.last_final_leader()
        if final_leader:
            self.tau_prime(final_leader)

    def tau_prime(self, key):
        if key is None or key in self.outputBlocks:
            return
        previous = self.previous_ratified_leader(key)
        self.tau_prime(previous)
        output = self.x_sort(key, self.closure(key) - self.closure(previous))
        logger.info(output)
        self.outputBlocks.extend(output)

    def dfs(self, head):
        order = []
        for kid in self.blocklace[head][POINTERS]:
            order = list(dict.fromkeys(order + self.dfs(kid)))
        return list(dict.fromkeys(order + (self.blocklace[head][POINTERS])))

    def x_sort(self, head, candidates):
        order = self.dfs(head) + [head]
        return list(sorted(candidates, key=order.index))

    def previous_ratified_leader(self, head):
        depth = self.blocklace[head][DEPTH] - 1
        children = set(self.blocklace[head][POINTERS])
        while depth >= 0:
            leader = self.leader(depth)
            depth_keys = {key for key in children if self.blocklace[key][DEPTH] == depth}
            leader_keys = {key for key in depth_keys if self.blocklace[key][CREATOR] == leader}
            for key in leader_keys:
                if self.ratifies(head, key):
                    return key
            grandchildren = {key for child in depth_keys for key in self.blocklace[child][POINTERS]}
            children -= depth_keys
            children |= grandchildren
            depth -= 1
        return None

    def last_final_leader(self):
        depth = self.completed_round()
        while depth >= 0:
            leader = self.leader(depth)
            if leader:
                leader_keys = [key
                               for key in self.blocklace
                               if self.blocklace[key][CREATOR] == leader and self.blocklace[key][DEPTH] == depth]
                for key in leader_keys:
                    if self.final_leader(key):
                        return key
            depth -= 1
        return None

    def final_leader(self, key):
        depth = self.blocklace[key][DEPTH]
        prefix = self.blocklace_prefix(depth, depth + self.wavelength)
        return self.super_ratifies(prefix, key)

    # Algorithm 3
    def receive(self, message):
        self.messages.append(message)
        completed = self.completed_round()
        logger.debug(f'completed round {completed} and I am in round {self.round}')
        if completed >= self.round:
            self.round = completed + 1
            block = self.create_block(self.messages)
            logger.debug(f'create block {block}')
            self.messages = []
            for agent in self.others:
                requests.post(f'http://localhost:{agent}/blocks', json=[block])
            self.buffer[block[HASHCODE]] = block
            while self.process_buffer():
                continue
            logger.debug(f'\nbuffer: {list(self.buffer.keys())}\naccepted: {list(self.blocklace.keys())}')

    def receive_block(self, block):
        if self.correct_block(block):
            self.buffer[block[HASHCODE]] = block
        while self.process_buffer():
            continue
        logger.debug(f'\ncreator {block[CREATOR]}\ndepth {block[DEPTH]}\nbuffer: {list(self.buffer.keys())}\naccepted: {list(self.blocklace.keys())}')

    def process_buffer(self):
        should_repeat = False
        to_delete = []
        for key in self.buffer:
            block = self.buffer[key]
            dangling_pointers = [pointer for pointer in block[POINTERS] if pointer not in self.blocklace]
            if not dangling_pointers and self.cordial_block(block):
                self.accept_block(block)
                to_delete.append(key)
                self.tau()
                should_repeat = True
        for key in to_delete:
            del self.buffer[key]
        return should_repeat and self.buffer

    def accept_block(self, block):
        key = block[HASHCODE]
        if key in self.blocklace:
            return False
        self.blocklace[key] = block
        self.max_depth = max(self.max_depth, block[DEPTH])
        creator = block[CREATOR]
        if creator in self.tips:
            tip = self.tips[creator]
            if tip in block[POINTERS]:
                self.tips[creator] = key
            else:
                self.equivocators[creator] = {self.tips[creator], key}
                del self.tips[creator]
        elif creator in self.equivocators:
            observed = {tip for tip in self.equivocators[creator] if tip in block[POINTERS]}
            self.equivocators[creator] -= observed
            self.equivocators[creator].add(key)
        elif block[DEPTH] == 0:
            self.tips[creator] = key

    def cordial_block(self, block):
        observed_agents = set()
        previous_round_count = 0
        bad_order = False
        for observed_key in block[POINTERS]:
            observed_block = self.blocklace[observed_key]
            observed_agents.add(observed_block[CREATOR])
            if observed_block[DEPTH] + 1 == block[DEPTH]:
                previous_round_count += 1
            elif observed_block[DEPTH] >= block[DEPTH]:
                bad_order = True
        a = len(observed_agents) == len(block[POINTERS])
        b = previous_round_count > self.super_majority
        c = block[DEPTH] == 0
        return (a and b and not bad_order) or c

    # Algorithm 4
    # TODO: the following should toss a shared coin. I hope that without byzantines round rubin will suffice
    def async_leader(self, depth):
        if depth % self.wavelength:
            return None
        return self.everyone[depth // len(self.everyone)]

    def async_completed_round(self):
        cycle = 0
        while self.cordial_round(cycle):
            cycle += 1
        return cycle-1

    def es_leader(self, depth):
        if depth % self.wavelength:
            return None
        return self.everyone[(depth // self.wavelength) % len(self.everyone)]

    # TODO: copying from async. We defer from the paper here
    def es_completed_round(self):
        cycle = 0
        while self.cordial_round(cycle):
            cycle += 1
        return cycle-1

# TODO: remove old blocks from buffer, so it won't explode
# TODO: verify hash codes
# TODO: check pointers when accepting a block to verify aligned front
# TODO: handle equivocating leaders
# TODO: add databases and persistency to make code server friendly
