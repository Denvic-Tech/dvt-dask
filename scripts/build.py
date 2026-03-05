import sys
import subprocess


def build():
    result = subprocess.run(["python", "-m", "build"], stdout=sys.stdout, stderr=sys.stderr)
    sys.exit(result.returncode)


if __name__ == "__main__":
    build()