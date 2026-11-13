import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
for sub in ("controller", "worker"):
    sys.path.insert(0, os.path.join(HERE, "..", sub))

