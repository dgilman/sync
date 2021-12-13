import subprocess
import sys
import os

# XXX multiprocessing
# XXX find the crash bug?:q

def main():
    search_dir = sys.argv[1]
    aacgain = os.getenv('AACGAIN')

    for dirpath, dirnames, filenames in os.walk(search_dir):
        if len(filenames) == 0:
            continue

        full_paths = [os.path.join(dirpath, filename) for filename in filenames]
        args = [aacgain, '-c', '-a', '-s', 'r', '-f', *full_paths]
        rval = subprocess.run(args)
        if rval.returncode != 0:
            print(f"Warning, failed run on {dirpath}")

if __name__ == "__main__":
    main()
