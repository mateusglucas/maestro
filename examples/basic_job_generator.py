import argparse
import asyncio
import random

import httpx

async def submit_jobs(server_url: str, n_jobs: int) -> None:
    async with httpx.AsyncClient(base_url=server_url) as client:
        for _ in range(n_jobs):
            delay = random.random() + 10
            response = await client.post(
                "/add_job",
                json={"parameters": {"delay": delay}},
            )
            response.raise_for_status()
            print(f"submitted delay={delay:.3f}s")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--n-jobs", type=int, default=10)
    parser.add_argument("--server-url", default="http://localhost:8000")
    args = parser.parse_args()

    asyncio.run(submit_jobs(args.server_url, args.n_jobs))


if __name__ == "__main__":
    main()
