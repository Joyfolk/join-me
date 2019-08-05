#!/usr/bin/env python3

import sys
import random
import hashlib

if len(sys.argv) < 3:
    print("usage: <lines_count> <id_range> [<seed>]")
    sys.exit(0)

gen_lines = int(sys.argv[1])
rand_range = int(sys.argv[2])

seed = None
if len(sys.argv) > 3:
    seed = sys.argv[3]

rng = random.Random(seed)

while gen_lines > 0:
    m = hashlib.md5()
    m.update(str(rng.randrange(rand_range)).encode('utf-8'))
    print("%d\t%s" % (rng.randrange(rand_range), m.hexdigest()))
    gen_lines -= 1
