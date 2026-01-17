import subprocess
from pathlib import Path
import sys
import ast

def run(cmd):
    subprocess.run(cmd, check=True)

#  Get arguments from CLI 
if len(sys.argv) != 4:
    print("Usage: python xyz.py <from_file> <to_file> <function_name>")
    sys.exit(1)

from_file = Path(sys.argv[1])
to_file = Path(sys.argv[2])
function_name = sys.argv[3]

#  Validate files 
if not from_file.exists():
    print(f"Source file not found: {from_file}")
    sys.exit(1)

if not to_file.exists():
    print(f"Destination file not found: {to_file}")
    sys.exit(1)

#  Read files 
from_text = from_file.read_text()
to_text = to_file.read_text()

#  Parse source file 
tree = ast.parse(from_text)
functions = {
    node.name: node
    for node in tree.body
    if isinstance(node, ast.FunctionDef)
}

if function_name not in functions:
    print(f"Function '{function_name}' not found in {from_file}")
    sys.exit(1)

target_func = functions[function_name]


# Prevent duplicate insert
if f"def {function_name}(" in to_text:
    print(f"Function '{function_name}' already exists in {to_file}")
    sys.exit(0)

# Extract function source 
from_lines = from_text.splitlines()
func_code = "\n".join(
    from_lines[target_func.lineno - 1 : target_func.end_lineno]
)

