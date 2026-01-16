import subprocess
from pathlib import Path
import sys
import ast

def run(cmd):
    subprocess.run(cmd, check=True)

# --- Get arguments from CLI ---
if len(sys.argv) != 4:
    print("Usage: python xyz.py <from_file> <to_file> <function_name>")
    sys.exit(1)

from_file = Path(sys.argv[1])
to_file = Path(sys.argv[2])
function_name = sys.argv[3]

