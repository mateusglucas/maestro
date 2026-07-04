# Examples

These examples show a minimal Maestro flow:

1. start the server;
2. start an agent with workers;
3. submit jobs for processing.

Run the commands below from this directory, because the examples load `server_config.toml` and `agent_config.toml` from the current working directory.

```bash
cd examples
```

## Files

| File | Description |
|------|-------------|
| `basic_server.py` | Creates the FastAPI server application. |
| `basic_agent.py` | Starts an agent that processes jobs with an artificial delay. |
| `basic_job_generator.py` | Submits jobs to the server. |
| `server_config.toml` | Local server configuration. |
| `agent_config.toml` | Local agent configuration. |

## 1. Start the server

In one terminal:

```bash
uv run uvicorn basic_server:app --reload --host 0.0.0.0 --port 8000
```

The server listens on `http://localhost:8000`.

## 2. Start the agent

In another terminal:

```bash
uv run python basic_agent.py
```

The agent registers with the server and starts requesting jobs to execute.

## 3. Submit jobs

In a third terminal:

```bash
uv run python basic_job_generator.py -n 10
```

To submit jobs to another server:

```bash
uv run python basic_job_generator.py --server-url http://localhost:8000 -n 10
```

Each submitted job contains a `delay` parameter, and the agent writes a simple artifact to the job output directory.

## Generated Outputs

During execution, the server may create local files such as:

- `test.db`, defined in `server_config.toml`;
- `artifacts/`, containing artifacts received from agents.
