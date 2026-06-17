from maestro.server import Api, create_app

app = create_app(Api())

# uv run uvicorn sample_server:app --reload --host 0.0.0.0 --port 8000
