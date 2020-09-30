#!/usr/bin/python3.6

import requests
import json
import time
import pyrebase
import datetime

#Este script faz requisições à API da Sefaz-AL e atualiza um banco de dados MySQL 
#para exibir onde é possível encontrar Álcool Gel na página https://www.agenciatatu.com.br/onde-tem-alcool-gel/

now = datetime.datetime.now()
diaatual = now.strftime("%d-%m-%Y")


#PRODUTO 1
produto = "alcool gel"

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2000) # faz 2000 tentativas de conexão evitando negações de serviço
session.mount('https://', adapter)
session.mount('http://', adapter)

payload = {"descricao": produto,  "dias": 3,  "latitude": -9.60258055556,  "longitude": -35.7593611111,  "raio": 15}
headers = {'AppToken': '913d079efe399ffbb4b4e72c92911061b71c8272', 'Content-Type': 'application/json'}
r = session.post("https://api.sefaz.al.gov.br/sfz_nfce_api/api/public/consultarPrecosPorDescricao", headers=headers, data=json.dumps(payload)) #json.dumps organiza o dict para que seja enviado um str com aspas duplas
print(r)
#print(r.text)

#PRODUTO 2
produto2 = "mascara cirurgica"

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2000) # faz 2000 tentativas de conexão evitando negações de serviço
session.mount('https://', adapter)
session.mount('http://', adapter)

payload = {"descricao": produto2,  "dias": 3,  "latitude": -9.60258055556,  "longitude": -35.7593611111,  "raio": 15}
headers = {'AppToken': 'TOKEN', 'Content-Type': 'application/json'}
r2 = session.post("https://api.sefaz.al.gov.br/sfz_nfce_api/api/public/consultarPrecosPorDescricao", headers=headers, data=json.dumps(payload)) #json.dumps organiza o dict para que seja enviado um str com aspas duplas
print(r2)
#print(r2.text)


#PRODUTO 3
produto3 = "veja multiuso"

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2000) # faz 2000 tentativas de conexão evitando negações de serviço
session.mount('https://', adapter)
session.mount('http://', adapter)

payload = {"descricao": produto3,  "dias": 3,  "latitude": -9.60258055556,  "longitude": -35.7593611111,  "raio": 15}
headers = {'AppToken': 'TOKEN', 'Content-Type': 'application/json'}
r3 = session.post("https://api.sefaz.al.gov.br/sfz_nfce_api/api/public/consultarPrecosPorDescricao", headers=headers, data=json.dumps(payload)) #json.dumps organiza o dict para que seja enviado um str com aspas duplas
print(r3)
#print(r3.text)

#PRODUTO 4
produto4 = "desinfetante"

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2000) # faz 2000 tentativas de conexão evitando negações de serviço
session.mount('https://', adapter)
session.mount('http://', adapter)

payload = {"descricao": produto4,  "dias": 3,  "latitude": -9.60258055556,  "longitude": -35.7593611111,  "raio": 15}
headers = {'AppToken': 'TOKEN', 'Content-Type': 'application/json'}
r4 = session.post("https://api.sefaz.al.gov.br/sfz_nfce_api/api/public/consultarPrecosPorDescricao", headers=headers, data=json.dumps(payload)) #json.dumps organiza o dict para que seja enviado um str com aspas duplas
print(r4)
#print(r4.text)


# In[62]:


#Carrega o json de cada produto e adiciona o tipo de produto buscado

listajson1 = json.loads(r.text)
x = 0
while x < len(listajson1):
    listajson1[x].update({"PRODUTO":"alcool"})
    x = x+1


listajson2 = json.loads(r2.text)
x = 0
while x < len(listajson2):
    listajson2[x].update({"PRODUTO":"mascara"})
    x = x+1


listajson3 = json.loads(r3.text)
x = 0
while x < len(listajson3):
    listajson3[x].update({"PRODUTO":"multiuso"})
    x = x+1



listajson4 = json.loads(r4.text)
x = 0
while x < len(listajson4):
    listajson4[x].update({"PRODUTO":"desinfetante"})
    x = x+1


#Junta todos num mesmo json


listajson1.extend(listajson2)
listajson1.extend(listajson3)
listajson1.extend(listajson4)

listajson = listajson1






import pandas as pd
df = pd.DataFrame(listajson)
#df['produto'] = produto

##SE NÃO TIVER NOME FANTASIA RECEBE A RAZÃO SOCIAL NO LUGAR
for index, row in df.iterrows():
    #print(row['nomFantasia'])
    if row['nomFantasia'] == None:
        #print(row['nomFantasia'])
        row['nomFantasia'] = row['nomRazaoSocial']
        #print(row['nomFantasia'])
        df['nomFantasia'][index] =  df['nomRazaoSocial'][index]

#df = df[df.PRODUTO.notnull()]


# In[64]:


import datetime

df['dthEmissaoUltimaVenda'] = pd.to_datetime(df.dthEmissaoUltimaVenda) - pd.Timedelta(hours=3) # Diminui 3 horas pois o horário da API está em +3 (GMT)


# In[120]:


df['dthEmissaoUltimaVenda'] = df['dthEmissaoUltimaVenda'].astype('datetime64[ns]')




import pymysql
import pymysql.cursors


# In[142]:


connection = pymysql.connect(host="NOSSOHOST.amazonaws.com", user="USUÁRIO", passwd="SENHA", db="DATABASE", autocommit=True)

now = datetime.datetime.now()
diaatual = (now - datetime.timedelta(hours= +3)).strftime("%d-%m-%Y|%H:%M:%S")

print("")
print("No horário do Brasil são: " , diaatual)
print("")

if connection:
    print ("Conectado ao mysql!\n")


for index, row in df.iterrows():

    mysql = 'INSERT INTO `table` (codGetin, codNcm, dscProduto, valMinimoVendido, valMaximoVendido, dthEmissaoUltimaVenda, valUnitarioUltimaVenda, valUltimaVenda, numCNPJ, nomRazaoSocial, nomFantasia, numTelefone, nomLogradouro, numImovel, nomBairro, numCep, nomMunicipio, numLatitude, numLongitude, PRODUTO) VALUES ("' + str(row['codGetin']) + '" ,"' + str(row['codNcm']) + '" ,"' + str(row['dscProduto']) + '" ,"' + str(row['valMinimoVendido']) + '" ,"' + str(row['valMaximoVendido']) +  '" ,"' + str(row['dthEmissaoUltimaVenda']) +  '" ,"' + str(row['valUnitarioUltimaVenda']) + '"  ,"' + str(row['valUltimaVenda']) + '"  ,"' + str(row['numCNPJ']) + '"  ,"' + str(row['nomRazaoSocial']) + '"  ,"' + str(row['nomFantasia']) + '"  ,"' + str(row['numTelefone']) + '"  ,"' + str(row['nomLogradouro']) + '"  ,"' + str(row['numImovel']) + '"  ,"' + str(row['nomBairro']) + '"  ,"' + str(row['numCep']) + '"  ,"' + str(row['nomMunicipio']) + '"  ,"' + str(row['numLatitude']) + '"  ,"' + str(row['numLongitude']) + '"  ,"' + str(row['PRODUTO']) + '");'
    print("Eis o Mysql")
    print(mysql)
    try:
        with connection.cursor() as cursor:
            sql = mysql
            cursor.execute(sql)
            results = cursor.fetchall()
            print("Resultado da query: ")
            print(results)
            for result in results:
                print(result)
            print("")
    except Exception as e:
        print("Erro na query: ")
        print(e)
        pass

connection.close()
