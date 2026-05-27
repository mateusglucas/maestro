# solicita novos jobs ao server, spawn de workers, distribui para workers e monitora status deles (salvar PID, creation time e job enviado)
# envia heartbeat pro server (que é substituível por qualquer outra requisição que seja feita)
# se worker morrer, spawn novo worker e redistribui o job dele pro novo worker
# workers são daemon do agente. se agente morrer, workers morrem
# agente pode redimensionar numero de workers soft (espera terminar trabalho pra encerrar) ou hard (mata processo na hora). como fornecer endpoint pra esse controle? fastapi ou CLI?
# worker sinaliza job completo pro agent. agent envia resultado pro server junto com artefatos (comprimidos em tarball)
# usa queue de dados compartilhada entre todos workers. quando precisa reduzir jobs, adiciona None na queue. Um worker que recebe None automaticamente se encerra, diminuindo numero de workers (isso pra redução soft). Redução hard tem que matar aleatoriamente algum worker.
# tem queue agent->workers e queue workers->agent, em que cada worker sinaliza pro agente quando pega um job específico, ou finaliza um job, ou se encerra.
# sem estados permanentes, sql..., tudo efêmero. Agent morre, workers morrem.
# ID do agente deve ter parcela que indique maquina (hostname?) e parcela UUID like, pro server poder identificar maquina e se o agent é novo ou não.

from abc import ABC, abstractmethod
import tomllib

class Agent(ABC):
    def __init__(self):
        self._init_config()

    def _init_config(self):
        with open("agent_config.toml", "rb") as f:
            self.config = tomllib.load(f)