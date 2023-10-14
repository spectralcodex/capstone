import sys
from app.sbdl import *
if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: sbdl {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_env = sys.argv[1].upper()
    sbdl = SBDL(job_env)

    sbdl.run()

