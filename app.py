# COMMAND ----------

import os
import time
import logging
import requests
from threading import Thread
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

# Configura o logging
logging.basicConfig(level=logging.INFO)

# --- 1. INICIALIZAÇÃO E CARREGAMENTO DE VARIÁVEIS ---
load_dotenv()

# Inicializa o App do Slack Bolt. Ele usa as variáveis de ambiente automaticamente.
app = App(
    token=os.environ.get("SLACK_BOT_TOKEN"),
    signing_secret=os.environ.get("SLACK_SIGNING_SECRET")
)

# Pega as credenciais do Databricks
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID")

# Headers para a API do Databricks
DBX_HEADERS = {"Authorization": f"Bearer {DATABRICKS_TOKEN}"}

# --- 2. FUNÇÃO PRINCIPAL QUE FAZ O TRABALHO PESADO ---
# Esta função será executada em uma thread separada para não bloquear o Slack
def processar_pergunta_genie(event, say):
    try:
        user_question = event.get("text", "").strip()
        channel_id = event.get("channel")
        thread_ts = event.get("ts") # 'ts' é o timestamp da mensagem original, usado para responder em thread

        # Remove a menção ao bot da pergunta do usuário
        # Ex: "<@U1234ABCD> qual a venda de ontem?" -> "qual a venda de ontem?"
        user_question_clean = user_question.split('>', 1)[-1].strip()
        
        logging.info(f"Pergunta recebida para o Genie: '{user_question_clean}' no canal {channel_id}")

        # Mensagem inicial para o usuário saber que estamos trabalhando
        say(text="Analisando sua pergunta com o Genie... :brain:", thread_ts=thread_ts)

        # --- CHAMADA À API DO DATABRICKS: INICIAR CONVERSA ---
        start_payload = {
            "messages": [{"role": "user", "content": user_question_clean}]
        }
        # start_url = f"{DATABRICKS_HOST}/api/2.0/genie/rooms/{GENIE_SPACE_ID}/start-conversation"
        start_url = f"{DATABRICKS_HOST}/ajax-api/2.0/data-rooms/{GENIE_SPACE_ID}/start-conversation"
        
        start_response = requests.post(start_url, headers=DBX_HEADERS, json=start_payload)
        start_response.raise_for_status() # Lança erro se a API retornar status != 2xx
        
        # --- CÓDIGO DE DEBUG - ADICIONE ESTAS 2 LINHAS ---
        logging.info(f"Status da resposta 'start': {start_response.status_code}")
        logging.info(f"Cabeçalhos da resposta 'start': {start_response.headers}")
        # --- FIM DO CÓDIGO DE DEBUG ---

        conversation_data = start_response.json()
        conversation_id = conversation_data.get("conversation_id")
        message_id = conversation_data.get("message_id")
        
        if not conversation_id or not message_id:
            raise ValueError("Não foi possível obter conversation_id ou message_id da API do Genie.")

        # --- POLLING: VERIFICAR O STATUS DA RESPOSTA ---
        # get_url = f"{DATABRICKS_HOST}/api/2.0/genie/rooms/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        get_url = f"{DATABRICKS_HOST}/ajax-api/2.0/data-rooms/{GENIE_SPACE_ID}/conversations/{conversation_id}/messages/{message_id}"
        
        final_answer = None
        max_retries = 10  # ~50 segundos de espera no máximo
        for _ in range(max_retries):
            get_response = requests.get(get_url, headers=DBX_HEADERS)
            get_response.raise_for_status()
            message_data = get_response.json()
            
            status = message_data.get("status")
            logging.info(f"Status da mensagem do Genie: {status}")

            if status == "SUCCEEDED":
                # A resposta pode vir em 'content.text' ou 'content.sql_code.text_output'
                final_answer = message_data.get("content", {}).get("text")
                break
            elif status == "FAILED":
                error_details = message_data.get("content", {}).get("error_details", "Erro desconhecido.")
                final_answer = f":x: Ocorreu um erro ao consultar o Genie: {error_details}"
                break
            
            time.sleep(5) # Espera 5 segundos antes de verificar novamente

        if not final_answer:
            final_answer = ":warning: A consulta ao Genie demorou muito para responder."

        # --- ENVIAR RESPOSTA FINAL AO SLACK ---
        # A função `say` do Bolt já sabe como enviar a mensagem para o canal correto e usar a thread.
        say(text=final_answer, thread_ts=thread_ts)

    except Exception as e:
        logging.error(f"Erro no processamento da thread: {e}")
        say(text=f":x: Desculpe, encontrei um erro interno: {e}", thread_ts=thread_ts)


# --- 3. EVENT LISTENER DO SLACK ---
# Escuta por menções ao bot em qualquer canal
@app.event("app_mention")
def handle_app_mention_events(event, say):
    # CRÍTICO: Inicia a função de processamento em uma thread e responde imediatamente.
    # O Slack exige uma resposta em 3 segundos. O processamento do Genie vai demorar mais.
    thread = Thread(target=processar_pergunta_genie, args=(event, say))
    thread.start()


# --- 4. CONFIGURAÇÃO DO SERVIDOR FLASK (PARA ROTEAMENTO) ---
# Necessário para hospedar o app Bolt em plataformas como Heroku ou VMs.
from flask import Flask, request

flask_app = Flask(__name__)
handler = SlackRequestHandler(app)

@flask_app.route("/slack/events", methods=["POST"])
def slack_events():
    return handler.handle(request)

# (Opcional) Rota raiz para verificar se o servidor está no ar
@flask_app.route("/", methods=["GET"])
def health_check():
    return "Serviço do Bot para o Genie está no ar!"

# Para rodar localmente:
# if __name__ == "__main__":
#     flask_app.run(port=int(os.environ.get("PORT", 3000)))

