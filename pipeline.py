import pandas as pd
import json

database = []
def extrair_dados(caminho_csv, caminho_json):
    # Tente escrever a lógica aqui sozinho
    df = pd.read_csv(caminho_csv, sep=';')
    with open(caminho_json, 'r', encoding='utf-8') as f: # ajuste no uft-8 formatação de texto
        database = json.load(f)
    ids_csv = df['UserID'].tolist()
    usuarios_filtrados = [user for user in database if user['id'] in ids_csv]
    
    return usuarios_filtrados, ids_csv
def transformar_dados(user):
    # Crie suas próprias regras de marketing aqui
    nome = user['name']
    saldo = user['account']['balance']
    limite = user['card']['limit']

    if saldo > 1000:
        texto = f"Olá {nome}! Seu saldo de R${saldo} está parado. Conheça nossas opções de CDB com liquidez diária!"
    elif limite > 4000:
        texto = f"Temos uma oferta VIP para você, {nome}! Use seu limite de R${limite} para compras em nossa loja com 10% de cashback."
    else:
        texto = f"{nome}, use o App Santander para organizar suas finanças e aumentar seu limite no futuro!"
    
    user['news'] = [{"icon": "https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg", "description": texto}]
    
    return texto
def carregar_dados(lista_transformada, nome_arquivo):
    # Salve o arquivo final
    with  open('santander_finalizado.json', 'w', encoding='utf-8') as f:
        json.dump(lista_transformada, f, ensure_ascii=False, indent=4)

    return f"Sucesso: Dados salvos em {nome_arquivo}"
def menu_exibição():

    print("--- Iniciando Pipeline ETL ---\n")
    database, user_ids = extrair_dados('santanderdevweek.csv', 'usuarios.json')
    print(f"1. Extração: {len(user_ids)} usuários encontrados (IDs: {user_ids})")
    [transformar_dados(user) for user in database]
    print(f"2. Transformação: Mensagens de marketing geradas com sucesso.")
    status = carregar_dados(database, 'santander_finalizado.json')
    print(f"3. Carregamento: {status}")

    print("--- Pipeline ETL Concluído ---\n")

    return database
def menu_mensagem(dados_exibir):
    if not dados_exibir:
        print("Nenhum dado disponível para exibição.")
        return
    for user in dados_exibir:   # Acessamos a mensagem limpa dentro da estrutura
        msg = user['news'][0]['description']    # Exibimos formatado: ID e Nome em cima, Mensagem embaixo
        print(f"CLIENTE: {user['name']} (ID: {user['id']})")
        print(f"MSG:     {msg}")
        print("-" * 60)  
    return msg
dados_finais = menu_exibição()
print('='*60)
menu_mensagem(dados_finais)