# Conteúdo do seu novo arquivo: test_local.py

import os
from dotenv import load_dotenv

# Importante: Importe a função que você quer testar do seu arquivo app.py
from app import processar_pergunta_genie

def main_teste():
    """
    Função principal que orquestra o teste local.
    """
    print("--- INICIANDO TESTE LOCAL DA LÓGICA DO GENIE ---")

    # 1. Carregar as variáveis de ambiente do arquivo .env
    #    Isso é crucial para que a função tenha acesso às chaves da API do Databricks.
    print("Carregando variáveis de ambiente do arquivo .env...")
    load_dotenv()
    
    # Verificação rápida se as chaves do Databricks foram carregadas
    if not os.environ.get("DATABRICKS_TOKEN"):
        print("\nERRO: Chave DATABRICKS_TOKEN não encontrada. Verifique seu arquivo .env")
        return

    # 2. Simular o objeto "event" que o Slack enviaria.
    #    Você pode mudar o valor de "text" para testar diferentes perguntas.
    mock_event = {
        "text": "<@U1234ABCD> qual a venda total do produto X no último trimestre?",
        "channel": "C_TESTE_LOCAL",
        "ts": "12345.6789" # Timestamp da "mensagem" para usar na thread de resposta
    }
    print(f"Simulando evento do Slack com a pergunta: '{mock_event['text']}'")

    # 3. Simular a função "say" que o Slack Bolt fornece.
    #    Nossa versão falsa apenas imprimirá a saída no terminal.
    def mock_say(text, thread_ts):
        print("\n-------------------------------------------")
        print(f"[RESPOSTA DO BOT para thread {thread_ts}]:")
        print(text)
        print("-------------------------------------------")

    # 4. EXECUTAR A FUNÇÃO PRINCIPAL!
    #    Chamamos a função importada, passando nossos objetos falsos.
    #    Como a função já roda em uma thread, não precisamos nos preocupar com isso aqui.
    #    (Nota: a função original do app.py já é projetada para ser executada em background)
    print("\nInvocando a função processar_pergunta_genie...")
    processar_pergunta_genie(event=mock_event, say=mock_say)
    print("\n--- TESTE LOCAL CONCLUÍDO ---")
    print("Aguarde a resposta da thread, que pode levar alguns segundos...")


if __name__ == "__main__":
    main_teste()