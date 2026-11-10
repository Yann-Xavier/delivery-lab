# Delivery Lab

O receptor gravou o pedido. A resposta se perdeu no caminho. Para o emissor, aquilo foi uma falha, e ele vai tentar de novo. O pedido vai ser gravado duas vezes?

Este laboratório junta os dois lados que o [Outbox Lab](https://github.com/Yann-Xavier/Outbox-Lab) e o [Webhook Lab](https://github.com/Yann-Xavier/webhook-lab) mostram separadamente: um emissor com Transactional Outbox e um receptor com deduplicação durável, conversando por HTTP. Um proxy entre os dois repassa a requisição e descarta a resposta.

```
emissor (outbox SQLite) --HTTP--> proxy com perda --HTTP--> receptor (FastAPI + SQLite)
```

## Como funciona

1. `record()` grava o evento e a entrega pendente na mesma transação.
2. `dispatch()` envia cada evento pendente uma vez, em ordem de gravação, e registra a tentativa em `attempts`. Uma falha não interrompe os demais eventos.
3. O receptor grava o evento com o ID como chave primária e guarda o hash SHA-256 do payload.
   - ID novo: `201 created`.
   - Mesmo ID e mesmo payload: `200 duplicate`, sem nova gravação.
   - Mesmo ID e payload diferente: `409 conflict`, e o registro original é mantido.
4. O emissor decide pelo resultado:

| Resposta | Resultado | O evento continua pendente? |
| --- | --- | --- |
| `200` ou `201` | `delivered` | Não |
| `409` | `conflict` | Não |
| outro `4xx` | `rejected` | Não |
| `5xx` | `server_error` | Sim |
| timeout, conexão recusada ou fechada | `transport_error` | Sim |

## Como executar

Requer Python 3.12 ou mais recente.

```sh
python -m pip install -r requirements.txt
```

Demonstração com três processos reais (receptor, proxy e emissor):

```sh
python demo.py
```

Saída esperada:

```
emissor: evento pedido-42 gravado na outbox
proxy: /events processado, resposta 201 descartada
1ª tentativa: [('pedido-42', 'transport_error')]
2ª tentativa: [('pedido-42', 'delivered')]

outbox do emissor:     [('pedido-42', 'delivered')]
tentativas do emissor: [('transport_error', 'RemoteProtocolError'), ('delivered', '200')]
eventos no receptor:   [('pedido-42',)]
```

Para subir as peças separadamente, em três terminais:

```sh
python -m uvicorn receiver:app --port 8001
python lossy_proxy.py --port 8002 --upstream http://127.0.0.1:8001 --drop 1
```

O emissor então usa `dispatch(caminho_do_banco, httpx.Client(), "http://127.0.0.1:8002/events")`.

## Testes

```sh
python -m pytest -q
```

| Teste | Falha simulada | O que verifica |
| --- | --- | --- |
| `test_lost_response_is_resent_and_applied_once` | Resposta perdida depois do processamento | Duas tentativas no emissor, um registro no receptor |
| `test_receiver_down_keeps_event_pending_until_it_returns` | Conexão recusada | O evento fica pendente e é entregue quando o receptor volta |
| `test_server_error_is_retried_and_client_error_is_not` | `503` e `400` | `5xx` continua pendente; `4xx` encerra a entrega |
| `test_same_id_with_different_payload_is_a_conflict` | Outro emissor reutiliza o ID com outro conteúdo | `409`, sem nova tentativa, payload original preservado |
| `test_sender_crash_after_send_resends_without_duplicate` | Processo do emissor morre (`os._exit`) depois do envio e antes de marcar a entrega | O reenvio não duplica o registro no receptor |
| `test_one_failure_does_not_block_other_events` | Primeiro evento perde a resposta | O segundo é entregue na mesma execução |
| `test_end_to_end_with_real_processes` | Proxy real fecha a conexão sem responder | O mesmo cenário da demonstração, com receptor e proxy em processos separados |

Os seis primeiros rodam o receptor no mesmo processo do teste. O último sobe o receptor com `uvicorn` e o proxy em processos separados e usa HTTP de verdade.

Com um receptor sem deduplicação (sem chave primária e sem `ON CONFLICT`), quatro testes falham: os de resposta perdida, conflito, queda do emissor e ponta a ponta.

## Escopo e limites

- **Um worker só.** Dois workers podem pegar o mesmo evento pendente. Esse caso é outro laboratório.
- **Sem backoff nem limite de tentativas.** Um evento que sempre recebe `5xx` fica pendente para sempre, e a próxima tentativa só acontece quando `dispatch()` é chamado de novo.
- **Tentativa interrompida não é registrada.** Se o emissor morre depois do envio, `attempts` não guarda essa tentativa. O teste de queda verifica isso explicitamente.
- **Sem autenticação.** O receptor aceita qualquer requisição.
- **Conflito é decisão de projeto.** O receptor mantém o primeiro payload e recusa o segundo. Em um sistema real, `409` costuma pedir investigação, e este laboratório não trata disso.
- **SQLite local e dados sintéticos.** O laboratório mostra a lógica, não o desempenho.

## Dados e privacidade

- Use apenas dados sintéticos. O laboratório não foi feito para receber dados reais de clientes.
- O receptor grava `id`, payload e hash de cada evento no arquivo indicado por `RECEIVER_DB`. O emissor grava eventos, entregas e tentativas no banco passado a `initialize()`. Não há retenção nem exclusão automática: para apagar os dados, remova os arquivos.
- `demo.py` cria os bancos em uma pasta temporária (`delivery-lab-*`), mostra o caminho no final e não a apaga.
- O proxy registra apenas o caminho da requisição e o status descartado, nunca o corpo. O receptor não registra payloads em log.
- O receptor não tem autenticação. Não o exponha fora da máquina local.
- Os arquivos `*.sqlite3` estão no `.gitignore`, para que bancos locais não sejam versionados.
