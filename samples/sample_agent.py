from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent.parent))

from agent import Agent
from time import sleep, time

def delay(artifact_path: Path, delay):
    start = time()
    sleep(delay)    
    end = time()

    file = artifact_path / 'dummy'

    file.write_text(f"delay: {delay}s")

    return {'start': start, 'end': end}

ag = Agent(delay)

ag.start()
ag.join()