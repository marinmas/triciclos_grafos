# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:01:17 2022

@author: -
"""

import sys
from pyspark import SparkContext
sc = SparkContext()

#Ordena lexicogrÃ¡ficamente los vÃ©rtices de cada arista (lÃ­nea del fichero)
def get_edges(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass #n1 == n2 y no queremos aÃ±adir bucles 
        
# Dada una tupla(nodo, lista de adyacencia) generamos la lista correspondiente de exists/pending
def funcion_auxiliar(tupla):
    result = []
    for i in range(len(tupla[1])):
        result.append(((tupla[0],tupla[1][i]),'exists'))
        for j in range(i+1,len(tupla[1])):
            if tupla[1][i] < tupla[1][j]:
                result.append(((tupla[1][i],tupla[1][j]),('pending',tupla[0])))
            else:
                result.append(((tupla[1][j],tupla[1][i]),('pending',tupla[0])))
    return result

#Filtra las aristas que puedan generar un triciclo    
def filtracion(tupla):
    return (len(tupla[1])>= 2 and 'exists' in tupla[1])


#Genera la terna correspondiente a un triciclo    
def coloca_ternas(tupla):
    result = []
    for elem in tupla[1]:
        if elem != 'exists':
            result.append((elem[1],tupla[0][0], tupla[0][1]))
    return result


    
    
def apartado2(sc, filelist):
    rdd = sc.parallelize([])
    for file in filelist:
        file_rdd = sc.textFile(file)
        rdd = rdd.union(file_rdd)
    adj = rdd.map(get_edges).filter(lambda x: x is not None).distinct()
    asociados = adj.groupByKey().mapValues(list).flatMap(funcion_auxiliar)
    triciclos = asociados.groupByKey().mapValues(list).filter(filtracion).flatMap(coloca_ternas)
    print(triciclos.collect())
    return triciclos.collect()

if __name__ == "__main__":
    lista = []
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        for i in range(len(sys.argv)):
            if i != 0:
                lista.append(sys.argv[i])
        apartado2(sc,lista)