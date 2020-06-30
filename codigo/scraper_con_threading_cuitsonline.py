#Esta es para scrapear cuitsonlinem, trabaja con threadings que permite ejecutar varios procesos al mismo tiempo. 
#Con una lista de 100 CUITS funcionó hoy 30/06 a las 00:34 hs. 
#Falta agregar lo de rotación de IPs para que cada threading pueda trabajar al mismo tiempo sin generar bloqueos por parte del website. 

#Principales librerias
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
%matplotlib inline
import seaborn as sns
import random
import time

cuits=pd.read_csv('cuits_prueba.csv', sep=',') #Si tienen el archivo guardado en local drive mandan el path ahí en vez del nombre del file. 
cuits=cuits.to_list()

# Cada link de cada empresa tiene la forma https://www.cuitonline.com/detalle/"NRO_DE_CUIT"/ 
#Creamos lista de links para scrapear
links=[]
a='https://www.cuitonline.com/detalle/'
for i in cuits:
  url=a+str(i)+'/'
  links.append(url)

#Librerias web requests y threading
import requests
from bs4 import BeautifulSoup
from time import sleep
import threading

#Fijamos la cantidad de hilos (threadings)
n_threads = 5 #fijo cinco pero se puede poner cualquier valor entre 1 y len(cuits)
cuits_and_codes=[] #This will be a list of tuples

#Vamos a determinar la cantidad de elementos que va a tener que analizar cada thread. 
p=len(links)//n_threads # Acá hacemos división entera para que me divida la cantidad de elementos en n partes iguales.
inicios = []
fines = []
inicio=0
fin=p


#Acá vamos a crear los inicios y los fines. 
for i in range(n_threads):
  inicios.append(inicio)
  fines.append(fin)
  inicio= inicio + p 
  fin= fin + p

def get_codes(inicio, fin): 
 
  #Fijamos headers
  headers = {'User-Agent': 
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
   
  h=0 #Indexador para armar lista de tuplas

  #Loop a través de la lista de links
  for i in range(inicio, fin):
  
    #Obtención de datos del links[i]. 
    page=links[i]
    
    tree = requests.get(page, headers = headers, timeout=10)
    
    soup = BeautifulSoup(tree.content, 'html.parser')
    
    spans=list(soup.find_all('span'))

    #Obtengo los elementos de la lista donde aparece 'tax-period'
    tax_period=[]
    #este for puede ser muy largo porq la pagina tiene muchos spans
    for i in spans:
      if "tax-period" in str(i):
        tax_period.append(i)

    #Obtengo los elementos de la lista donde aparece 'activity-order'
    activity_order=[]
    for i in tax_period:
      if "activity-order" in str(i):
        activity_order.append(i)
        
    #Obtengo los elementos de la lista donde aparece un número. 
    codes=[]
    for i in activity_order:
      for j in str(i).split():
        if str(j).isnumeric()==True:
          codes.append(j)

    #Appendeo los codes para cada cuit (ACA ARMO LA LISTA DE TUPLAS)
    cuits_and_codes.append((cuits[inicio+h],codes))
    h=h+1
        
    #Genero pausa entre ciclos. (ESTO LO HAGO PARA QUE CUITONLINE NO ME BLOQUEE) 
    time.sleep(2)

  return cuits_and_codes
  
#Acá le pido al módulo threading que me ejecute la función en threadings

threads=[]
for i in range(len(inicios)):
  t=threading.Thread(target=get_codes, args=(inicios[i], fines[i],))
  threads.append(t)
  t.start()

for t in threads:
  t.join()

cuits_and_codes #Este es el resultado que me joinea

#Armo el DataFrame
df=pd.DataFrame(cuits_and_codes, columns=['cuit','codes']).explode('codes')
df.reset_index(drop=True, inplace=True)

#Exporto a CSV
df.to_csv('cuits_y_codigos.csv', sep=';', index=False)
