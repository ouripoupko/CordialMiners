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
        self.round = 0
        self.round_collection = {}
        self.next_non_final_round = 0
        self.max_depth = 0
        self.buffer = {}
        self.blocklace = {}
        self.tips = {}
        self.equivocators = {}
        self.wavelength = 3
        self.leader = self.es_leader
        self.final_leader = self.es_final_leader
        self.completed_round = self.es_completed_round
        self.leader_collection = {}
        self.previous_final_leader = None
        self.outputBlocks = {}
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
                 POINTERS: [key for key in self.blocklace if self.blocklace[key][DEPTH] == self.round],
                 DEPTH: self.round + 1}
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
        block = self.blocklace[head]
        children = set(block[POINTERS])
        reply = {head}
        while children:
            reply.update(children)
            children = {grand_child for child in children for grand_child in self.blocklace[child][POINTERS]}
        return reply

    # TODO: make sure we don't use this horrible function
    def equivocation(self, b1, b2):
        c1 = self.blocklace[b1][CREATOR] == self.blocklace[b2][CREATOR]
        c2 = self.observes(b1, b2)
        c3 = self.observes(b2, b1)
        return c1 and not c2 and not c3

    def equivocator(self, q, b):
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
        return a and b and c and d and (e or g)

    # TODO: check - should a block approve itself?
    def approves(self, head, block):
        children = set(self.blocklace[head][POINTERS])
        creator = self.blocklace[block][CREATOR]
        # instead of checking every block in the closure of 'head', if it equivocates 'block'
        # check all tips of the equivocator. if a tip observes 'block' it is safe
        # if it doesn't, enough to check if the leaf of that tip is in the closure of 'head'
        equivocating_blocks = set()
        if creator in self.equivocators:
            equivocating_blocks = {self.leaf_of_creator(tip)
                                   for tip in self.equivocators[creator]
                                   if not self.observes(tip, block)}
        in_tree = False
        equivocate = False
        while children and not equivocate:
            equivocate = not children.isdisjoint(equivocating_blocks)
            if block in children:
                in_tree = True
            children = {grand_child for child in children for grand_child in self.blocklace[child][POINTERS]}
        return in_tree and not equivocate

    def ratifies(self, head, block):
        depth = self.blocklace[block][DEPTH]
        approvers = set()
        observers = {head}
        while observers:
            observer = observers.pop()
            if self.approves(observer, block):
                approvers.add(self.blocklace[observer][CREATOR])
            observers.update({child
                              for child in self.blocklace[observer][POINTERS]
                              if self.blocklace[child][DEPTH] >= depth})
        return len(approvers) > self.super_majority

    def super_ratifies(self, head, block):
        depth = self.blocklace[block][DEPTH]
        ratifiers = set()
        observers = {head}
        while observers:
            observer = observers.pop()
            if self.ratifies(observer, block):
                ratifiers.add(self.blocklace[observer][CREATOR])
                observers.update({child
                                  for child in self.blocklace[observer][POINTERS]
                                  if self.blocklace[child][DEPTH] >= depth})
        return len(ratifiers) > self.super_majority

    def blocklace_prefix(self, depth):
        return {block for block in self.blocklace if block[DEPTH] <= depth}

    def cordial_round(self, cycle):
        creators = {block[CREATOR] for block in self.blocklace.values() if block[DEPTH] == cycle}
        return len(creators) > self.super_majority

    # Algorithm 2
    def tau(self, block):
        final_leader = self.last_final_leader(block)
        if final_leader:
            self.tau_prime(final_leader)

    def tau_prime(self, block):
        if block is None or block in self.outputBlocks:
            return
        previous = self.previous_ratified_leader(block)
        self.tau_prime(previous)
        output = self.xsort(block, self.closure(block) - self.closure(previous))
        logger.debug(output)
        self.outputBlocks.update(output)

    def xsort(self, head, candidates):
        # TODO: unclear how to sort
        return list(candidates, key=lambda x: self.blocklace[x][DEPTH])

    def arg_max_depth(self, blocks):
        max_depth = 0
        reply = None
        for key in blocks:
            if self.blocklace[key][DEPTH] > max_depth:
                max_depth = self.blocklace[key][DEPTH]
                reply = self.blocklace[key]
        return reply

    def previous_ratified_leader(self, head):
        R = {block
             for block in self.closure(head)
             if block is not head
             and self.ratifies(head, block)
             and self.blocklace[block][CREATOR] == self.leader(self.blocklace[block][DEPTH])}
        return self.arg_max_depth(R)

    def last_final_leader(self, block):
        depth = block(DEPTH)
        previous_final = self.blocklace.get(self.previous_final_leader)
        if previous_final and depth <= previous_final[DEPTH]:
            return None
        leader_id = self.leader(depth)
        if leader_id == block[CREATOR]:
            self.leader_collection[depth] = {'leader': block[HASHCODE], 'ratifications': {}, 'super': {}}
            return None
        wave_start = depth - depth % self.wavelength
        if wave_start not in self.leader_collection:
            return None
        collection = self.leader_collection[wave_start]
        leader_key = collection['leader']
        leader_block = self.blocklace[leader_key]
        if block[CREATOR] not in collection['ratifications']:
            if self.approves(block, leader_block):
                collection['ratifications'][block[CREATOR]] = block[HASHCODE]
        if len(collection['ratifications']) <= self.super_majority:
            return None
        if block[CREATOR] not in collection['super']:
            if self.ratifies(block, {self.blocklace[key] for key in collection['ratifications'].values()}):
                collection['super'][block[CREATOR]] = block[HASHCODE]
        if len(collection['super']) <= self.super_majority:
            return None
        return leader_block


    # Algorithm 3
    def receive(self, message):
        self.messages.append(message)
        completed = self.completed_round()
        logger.debug(f'completed round {completed} and I am in round {self.round}')
        if completed >= self.round:
            block = self.create_block(self.messages)
            logger.debug(f'create block {block}')
            self.messages = []
            self.round = block[DEPTH]
            logger.debug(f'send it to {self.others}')
            for agent in self.others:
                requests.post(f'http://localhost:{agent}/blocks', json=[block])
            self.buffer[block[HASHCODE]] = block
            while self.process_buffer():
                continue

    def receive_block(self, block):
        if self.correct_block(block):
            self.buffer[block[HASHCODE]] = block
        while self.process_buffer():
            continue

    def process_buffer(self):
        should_repeat = False
        for key in self.buffer:
            block = self.buffer[key]
            dangling_pointers = [pointer for pointer in block[POINTERS] if pointer not in self.blocklace]
            if not dangling_pointers and self.cordial_block(block):
                self.accept_block(block)
                del self.buffer[key]
                self.tau(block)
                should_repeat = True
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

    def async_final_leader(self, block):
        depth = block[DEPTH] + self.wavelength - 1
        prefix = self.blocklace_prefix(depth)
        Blocks = {ratifier for ratifier in prefix if self.ratifies(ratifier, block)}
        creators = {ratifier[CREATOR] for ratifier in Blocks}
        return len(creators) > self.super_majority

    def async_completed_round(self):
        cycle = 1
        while self.cordial_round(cycle):
            cycle += 1
        return cycle-1

    def es_leader(self, depth):
        if depth % self.wavelength:
            return None
        return self.everyone[(depth / self.wavelength) % len(self.everyone)]

    def es_final_leader(self, block):
        depth = block[DEPTH] + self.wavelength
        prefix = self.blocklace_prefix(depth)
        ratifiers = {candidate for candidate in prefix if self.ratifies(candidate, block)}
        creators = {ratifier[CREATOR] for ratifier in ratifiers}
        finals = {}
        if len(creators) > self.super_majority:
            tops = {ratifier for ratifier in ratifiers if self.blocklace[ratifier][DEPTH] == depth}
            leader = self.leader(depth)
            finals = {ratifier for ratifier in tops if self.blocklace[ratifier][CREATOR] == leader}
        return len(finals) > 0

    # TODO: copying from async. We defer from the paper here
    def es_completed_round(self):
        cycle = 1
        while self.cordial_round(cycle):
            cycle += 1
        return cycle-1

# TODO: remove old blocks from buffer, so it won't explode
# TODO: verify hash codes
# TODO: check pointers when accepting a block to verify aligned front
# TODO: handle equivocating leaders
# TODO: add databases and persistency to make code server friendly
