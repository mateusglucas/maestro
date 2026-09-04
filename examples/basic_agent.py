from maestro.agent import Agent
from time import sleep, time
from pathlib import Path

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
