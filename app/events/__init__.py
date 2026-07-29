import os
import sys

# Ensure generated protobuf files can import each other relative to this directory
sys.path.insert(0, os.path.dirname(__file__))
