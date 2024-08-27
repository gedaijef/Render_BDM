# Código para Ler, Categorizar e Armazenar no Banco de Dados

# Importações
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
import os
from dotenv import load_dotenv
import requests
import datetime
from time import sleep
import time
import json
import psycopg2

# Carregando as envs
load_dotenv()

# Conexão da GREEN API
url = os.getenv('URL_LER')
url_enviar = os.getenv('URL_ENVIAR')
headers = {}
headers_enviar = {'Content-Type': 'application/json'}

# Especificações da LLM
llm = ChatOpenAI(
    model="gpt-4o",
    temperature=0.5,
    api_key=os.getenv('OPENAI_API_KEY')
)

# Leitura do arquivo das Categorias
with open("categorias.txt", encoding="utf-8") as arquivo:
    categorias = arquivo.read()

# Função para Categorizar
def categorizar_noticias(llm, template, categorias, noticia):
    prompt = template.format(categorias=categorias, noticia=noticia)
    resposta = llm.invoke(prompt)
    print(resposta.content)
    return resposta.content

# Função para inserir registro no BD
def inserir_registro(query_sql):
    conn = psycopg2.connect(os.getenv('URL_BD'))
    cur = conn.cursor()
    cur.execute(query_sql)
    conn.commit()
    print('Registro adicionado.')
    conn.close()

# Função para selecionar os números atrelados à categoria
def selecionar(query):
    conn = psycopg2.connect(os.getenv('URL_BD'))
    cur = conn.cursor()
    cur.execute(query)
    lista_selects = cur.fetchall()
    conn.close()
    return lista_selects

# Repetição para ler, categorizar e armazenar as notícias
while True:
    response = requests.get(url, headers=headers)
    print(response.status_code)

    comeco = time.time()
    
    if response.status_code == 200:
        messages = json.loads(response.text)
        
        for message in messages:
            # Filtrar apenas as mensagens do grupo especificado
            if message['chatId'] == os.getenv('CHAT_ID'):

                # Formatar data e hora
                data = datetime.datetime.fromtimestamp(message['timestamp'])
                data_formt = str(data.strftime('%Y-%m-%d'))
                hora_formt = str(data.strftime('%H:%M:%S'))
                noticia = message['textMessage']

                # Prompt para a Classificação
                template = ChatPromptTemplate.from_template(f"""
Seu papel é categorizar notícias do ramo de investimentos com base em categorias de notícias pré-definidas.
As categorias são: {categorias}.

Classificação:
Saída: Deve conter somente o nome da categoria, ou seja, o que aparece antes do ":". Exemplo de saída: Se a notícia se encaixou em "Mercados do Brasil: Ibovespa - Câmbio - Juros", a saída deve ser apenas "Mercados do Brasil".
Palavras-chave: Utilize as palavras-chave fornecidas após os “:” de cada categoria como parâmetros para a classificação.
Priorização: Dê preferência à categoria com maior número de palavras-chave presentes na notícia.

Considerações:
Ambiguidades: Se a notícia se encaixar em duas categorias, escolha a mais relevante. Em caso de dúvida, priorize a classificação mais específica.
Múltiplas categorias ou nenhuma categoria: Se a notícia não se encaixar em nenhuma ou em mais de duas categorias, retorne “Não foi possível categorizar”. Estes são os únicos casos que você pode retornar algo que não seja o nome da categoria.

Exemplo de múltiplas categorias: 
As manchetes desta 6ªF, 28/6/2024 

VALOR 
▪️Críticas de Lula dificultam controle da inflação, diz Campos Neto 
▪️Ex-executivos da Americanas são alvos de operação da PF     

GLOBO 
▪️Ex-diretores da Americanas lucraram com fraude, aponta PF   
▪️Mau desempenho de Biden preocupa democratas 

FOLHA 
▪️Ex-chefes da Americanas têm prisão decretada por fraude 
▪️Lula elogia Galípolo e agora diz que é possível cortar despesas  

ESTADÃO 
▪️Fraude na Americanas era sistemática e chamada de 'solução criativa', diz delação  
▪️Nunes, Boulos e Datena têm empate técnico em SP, aponta pesquisa

Saída esperada: Múltiplas Categorias

Exemplo de notícia padrão:
++ PIB final britânico do 1ºTri avança +0,3% na comparação anual, ligeiramente acima do consenso (+0,2%)

++ Na comparação com o trimestre anterior, o crescimento foi de +0,7%, também um pouco acima da expectativa de +0,6%

Saída esperada: Indicadores econômicos

Não crie novas categorias. Retorne somente o nome da categoria.
Após retornar a categoria atrelada a cada notícia, não acrescente nada.

Categorize a seguinte notícia: {noticia}
""")
            
                categoria = categorizar_noticias(llm, template, categorias, noticia)
                
                # Inserir query sql com os parâmetros definidos
                query_sql = f"INSERT INTO mensagem (mensagem, data, hora, categoria) VALUES ('{noticia}', '{data_formt}', '{hora_formt}', '{categoria}')"
                
                try:
                    inserir_registro(query_sql)
                except:
                    print('Caiu na exceção 1.')
                    sleep(60)
                    continue

                print(f"Data: {data_formt}, Hora: {hora_formt}, Notícia: {noticia}, Categoria: {categoria}")

                # Selecionar os números que são da categoria da notícia
                numeros = selecionar(f"select cliente.numero from cliente join categoria_cliente on cliente.codigo = categoria_cliente.cliente_codigo join categoria on categoria.codigo = categoria_cliente.categoria_codigo where categoria.nome = '{categoria}'")
                
                for numero in numeros:
                    numero = numero[0]
                    numero +='@c.us'
                    payload = {
                    "chatId": numero, 
                    "message": noticia
                    }

                    # Enviar mensagens para os números
                    response = requests.request("POST", url_enviar, data=json.dumps(payload), headers=headers_enviar)
                    print(response.text.encode('utf8'))
                
                query_sql = f"update mensagem set enviada = true where mensagem = '{noticia}'"

                try:
                    inserir_registro(query_sql)    
                except:
                    print('Caiu na exceção 2.')
                    sleep(60)
                    continue     

    termino = time.time() 

    if (termino-comeco<60):
        sleep(60-(termino-comeco))

