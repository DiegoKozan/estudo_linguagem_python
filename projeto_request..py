import requests
dados = requests.get('https://servicodados.ibge.gov.br/api/v2/cnae/classes').json() #lista para dicionaros
dados  [0] # exibindo primeiro registro de dados(primeiro dicionario da lista)

{'id': '01113',
 'descricao': 'CULTIVO DE CEREAIS',
 'grupo': {'id': '011',
  'descricao': 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
  'divisao': {'id': '01',
   'descricao': 'AGRICULTURA, PECUÁRIA E SERVIÇOS RELACIONADOS',
   'secao': {'id': 'A',
    'descricao': 'AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA'}}},
 'observacoes': [
  'Esta classe compreende - o cultivo de alpiste, arroz, aveia, centeio, cevada, milho, milheto, painço, sorgo, trigo, trigo preto, triticale e outros cereais não especificados anteriormente',
  'Esta classe compreende ainda - o beneficiamento de cereais em estabelecimento agrícola, quando atividade complementar ao cultivo\r\n- a produção de sementes de cereais, quando atividade complementar ao cultivo',
  'Esta classe NÃO compreende - a produção de sementes certificadas dos cereais desta classe, inclusive modificadas geneticamente (01.41-5)\r\n- os serviços de preparação de terreno, cultivo e colheita realizados sob contrato (01.61-0)\r\n- o beneficiamento de cereais em estabelecimento agrícola realizado sob contrato (01.63-6)\r\n- o processamento ou beneficiamento de cereais em estabelecimento não-agrícola (grupo 10.4) e (grupo 10.6)\r\n- a produção de biocombustível (19.32-2)']}
# Quntidae  de distintas de atividades , basta saber o tamanho da lista .

qtde_atividades_distintas = len(dados)

#Criando uma lista dos grupos de atividades, extraindo

grupos=[]
for registro in dados:
    grupos.append(registro['grupo']['descricao'])

grupos  [:10]
['PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'PRODUÇÃO DE LAVOURAS TEMPORÁRIAS',
 'HORTICULTURA E FLORICULTURA',
 'HORTICULTURA E FLORICULTURA',
 'EXTRAÇÃO DE MINERAIS METÁLICOS NÃO-FERROSOS']
# A partir da lista, podemos extrair a quantidade de grupos de atividades

qtde_grupos_distintos = len(set(grupos)) # o construtor set cria uma estrutura de dados removendo as duplicações.
# Resultado é uma lista de tuplas. Cria uma nova lista com o grupo e a quantidade de atividades pertencentes a ele

grupos_count = [(grupo, grupos.count(grupo)) for grupo in set(grupos)]
grupos_count[:5]
[('TECELAGEM, EXCETO MALHA', 3),
 ('COMÉRCIO ATACADISTA DE PRODUTOS DE CONSUMO NÃO-ALIMENTAR', 8),
 ('ATIVIDADES DE ORGANIZAÇÕES ASSOCIATIVAS PATRONAIS, EMPRESARIAIS E PROFISSIONAIS',
  2),
 ('SEGURIDADE SOCIAL OBRIGATÓRIA', 1),
 ('FABRICAÇÃO DE ELETRODOMÉSTICOS', 2)]
# Por conveniência, transformamos a lista em um dicionário

grupos_count = dict(grupos_count)
# A partir do dicionário vamos descobrir qual (ou quais) grupos possuem mais atividades

valor_maximo = max(grupos_count.values())
grupos_mais_atividades = [chave for (chave, valor) in grupos_count.items() if valor == valor_maximo]
print(len(grupos_mais_atividades))
grupos_mais_atividades

['REPRESENTANTES COMERCIAIS E AGENTES DO COMÉRCIO, EXCETO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS']

import requests

from datetime import datetime


class ETL:
 def __init__(self):
  self.url = None

 def extract_cnae_data(self, url):
  self.url = url
  data_extracao = datetime.today().strftime("%Y/%m/%d - %H:%M:%S")
  # Faz extração
  dados = requests.get(self.url).json()

  # Extrai os grupos dos registros
  grupos = []
  for registro in dados:
   grupos.append(registro['grupo']['descricao'])

  # Cria uma lista de tuplas (grupo, quantidade_atividades)
  grupos_count = [(grupo, grupos.count(grupo)) for grupo in set(grupos)]
  grupos_count = dict(grupos_count)  # transforma a lista em dicionário

  valor_maximo = max(grupos_count.values())  # Captura o valor máximo de atividades
  # Gera uma lista com os grupos que possuem a quantidade máxima de atividades
  grupos_mais_atividades = [chave for (chave, valor) in grupos_count.items() if valor == valor_maximo]

  # informações
  qtde_atividades_distintas = len(dados)
  qtde_grupos_distintos = len(set(grupos))

  print(f"Dados extraídos em: {data_extracao}")
  print(f"Quantidade de atividades distintas = {qtde_atividades_distintas}")
  print(f"Quantidade de grupos distintos = {qtde_grupos_distintos}")
  print(f"Grupos com o maior número de atividades = {grupos_mais_atividades}, atividades = {valor_maximo}")

  return None

# Usando a classe ETL

ETL().extract_cnae_data('https://servicodados.ibge.gov.br/api/v2/cnae/classes')