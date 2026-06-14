# Maestro

Maestro é um sistema de orquestração de jobs distribuídos. Um **server** central enfileira trabalhos, registra **agents** (máquinas que executam tarefas) e recebe resultados. Cada **agent** mantém um pool de **workers** em processos separados que processam jobs em paralelo.

## Arquitetura

```
                    ┌─────────────┐
                    │   Cliente   │
                    │ (add_job)   │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │   Server    │  SQLite (jobs, agents)
                    │  (FastAPI)  │
                    └──────┬──────┘
                           │ HTTP
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐ ┌───▼──────┐ ┌───▼──────┐
       │   Agent 1   │ │ Agent 2  │ │ Agent N  │
       │  (workers)  │ │ (workers)│ │ (workers)│
       └─────────────┘ └──────────┘ └──────────┘
```

**Fluxo básico:**

1. Um cliente envia um job para o server (`POST /add_job`).
2. O agent se registra no server e envia heartbeats periódicos.
3. Workers ociosos pedem trabalho ao server (`POST /request_job/{agent_id}`).
4. O worker executa a função de trabalho definida pelo usuário.
5. O agent envia o resultado (e artefatos opcionais) de volta ao server (`POST /submit_result/...`).

Se um worker morrer no meio de um job, o agent detecta a falha, devolve o job ao estado pendente e pode redistribuí-lo. Se um agent parar de enviar heartbeat, o server pode reatribuir jobs travados a outro agent.

## Componentes

| Arquivo | Papel |
|---------|-------|
| `server.py` | API FastAPI, modelos SQLAlchemy (`Job`, `Agent`), fila e persistência |
| `agent.py` | Classe base `Agent`: heartbeat, pool de workers, fila local de jobs |
| `common.py` | Tipos compartilhados entre server e agent (`JobStatus`, `JobResults`) |
| `server_config.toml` | URL do banco e timeout de heartbeat do server |
| `agent_config.toml` | URL do server, intervalo de heartbeat e número de workers |
| `test_server.py` | Entry point ASGI do server |
| `test_agent.py` | Exemplo mínimo de agent com função de trabalho vazia |

## API do server

| Método | Rota | Descrição |
|--------|------|-----------|
| `POST` | `/register_agent` | Registra um agent (`{"hostname": "..."}`) e retorna `agent_id` |
| `POST` | `/heartbeat/{agent_id}` | Atualiza `last_seen` do agent |
| `POST` | `/request_job/{agent_id}` | Atribui o próximo job pendente (ou job de agent inativo) |
| `POST` | `/submit_result/{agent_id}/{job_id}` | Envia resultado (`results` como JSON em form field) e artefato opcional |
| `POST` | `/add_job` | Enfileira um job (`{"parameters": {...}}`) |

### Status de job

`PENDING` → `ASSIGNED` → `COMPLETED` | `FAILED` | `CANCELLED`

## Configuração

### Server (`server_config.toml`)

```toml
[database]
url = "sqlite+aiosqlite:///test.db"

[heartbeat]
timeout = 300  # segundos sem heartbeat antes de considerar agent inativo
```

### Agent (`agent_config.toml`)

```toml
[server]
url = "http://localhost:8000"

[heartbeat]
interval = 150  # segundos entre heartbeats

[jobs]
n_workers = 4   # workers em paralelo por agent
```

## Requisitos

- Python 3.12+
- Dependências principais: FastAPI, Uvicorn, SQLAlchemy, httpx, aiofiles, aiosqlite, python-multipart, pyzstd

## Instalação

```bash
python3 -m venv .venv
source .venv/bin/activate

uv pip install fastapi uvicorn sqlalchemy aiosqlite httpx aiofiles python-multipart pyzstd
```

## Uso

### 1. Subir o server

```bash
uvicorn test_server:app --reload --host 0.0.0.0 --port 8000
```

### 2. Implementar e rodar um agent

Defina uma função de trabalho que recebe um diretório de artefatos e os parâmetros do job:

```python
from agent import Agent
from pathlib import Path

def work(artifact_path: Path, **parameters):
    # Escreva saídas em artifact_path se necessário
    output_file = artifact_path / "result.txt"
    output_file.write_text(f"Processado: {parameters}")
    return {"message": "ok"}

agent = Agent(work)
agent.start()
agent.join()
```

### 3. Enviar um job

```bash
curl -X POST http://localhost:8000/add_job \
  -H "Content-Type: application/json" \
  -d '{"parameters": {"input": "exemplo"}}'
```

Artefatos gerados pelo worker são compactados em tarball `.tar.zst` e enviados ao server, que os grava em `artifacts/`.

## Modelo de execução do agent

- O agent roda em uma thread principal com três corrotinas: heartbeat, monitor de workers e loop de eventos.
- Workers são processos filho (`multiprocessing.Process`) que consomem jobs de filas locais.
- O agent mantém um SQLite efêmero em diretório temporário para rastrear jobs recebidos e workers ativos.
- Redução soft do pool: quando `n_workers` diminui na config, workers ociosos recebem um job sentinela (`job_id: None`) e encerram gracefully.

## Estado do projeto

Maestro está em desenvolvimento ativo. Alguns pontos ainda em evolução:

- Controle dinâmico do número de workers (endpoint ou CLI)
- Gerador de jobs de teste (`test_jobs_generator.py`)
- Correções menores pendentes no agent (ex.: status de sucesso em `_send_completed_result`)

## Estrutura do repositório

```
maestro/
├── server.py              # Server e modelos de dados
├── agent.py               # Agent e workers
├── common.py              # Tipos compartilhados
├── server_config.toml
├── agent_config.toml
├── test_server.py         # App ASGI
├── test_agent.py          # Exemplo de agent
├── test_jobs_generator.py # (WIP) utilitário para enfileirar jobs
└── artifacts/             # Artefatos recebidos pelo server (criado em runtime)
```
