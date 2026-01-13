import subprocess
from pathlib import Path
import sys

FILE = Path("etl.py")

lines = FILE.read_text().splitlines()
changed = False

i = 0
while i < len(lines):
    if lines[i].startswith("# STEP"):
        i += 1

        # Find the next commented def
        while i < len(lines):
            line = lines[i]
            stripped = line.lstrip()
            if stripped.startswith("# def"):
                # Uncomment the def line
                hash_index = line.index("#")
                # Remove only the # and one space if present
                if line[hash_index + 1:hash_index + 2] == " ":
                    lines[i] = line[:hash_index] + line[hash_index + 2 :]
                else:
                    lines[i] = line[:hash_index] + line[hash_index + 1 :]
                i += 1

                # Uncomment all following commented lines until empty or uncommented
                while i < len(lines):
                    line = lines[i]
                    stripped = line.lstrip()
                    if stripped == "" or not stripped.startswith("#"):
                        break
                    hash_index = line.index("#")
                    # Remove only # and one space after it if exists
                    if line[hash_index + 1:hash_index + 2] == " ":
                        lines[i] = line[:hash_index] + line[hash_index + 2 :]
                    else:
                        lines[i] = line[:hash_index] + line[hash_index + 1 :]
                    i += 1

                changed = True
                break
            else:
                i += 1
        break
    i += 1

if not changed:
    print("No more functions to activate.")
    sys.exit(0)

# Write back the file
FILE.write_text("\n".join(lines) + "\n")
print("Function uncommented successfully!")

def run(cmd):
    subprocess.run(cmd, check=True)