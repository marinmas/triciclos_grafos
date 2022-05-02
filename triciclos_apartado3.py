# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:02:03 2022

@author: -
"""

import sys
from pyspark import SparkContext
sc = SparkContext()

#Ordena lexicogrÃ¡ficamente los vÃ©rtices de cada arista (lÃ­nea del fichero) y cada nodo lleva asociado el grafo del que proviene
def get_edges1(line,filename):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return ((n1,filename),(n2,filename))
    elif n1 > n2:
         return ((n2,filename),(n1,filename))
    else:
        pass #n1 == n2 y no queremos aÃ±adir bucles

# Dada una tupla(nodo, lista de adyacencia) generamos la lista correspondiente de exists/pending
def funcion_auxiliar1(tupla):
    result = []
    for i in range(len(tupla[1])):
        result.append(((tupla[0],tupla[1][i]),'exists'))
        for j in range(i+1,len(tupla[1])):
            if tupla[1][i][0] < tupla[1][j][0]:
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


    
def apartado3(sc, filelist): #nos va a devolver los triciclos con el nombr
    rdd = sc.parallelize([])
    for file in filelist:
        file_rdd = sc.textFile(file).\
            map(lambda a : get_edges1(a,file)).\
            filter(lambda x: x is not None).\
            distinct()
        rdd = rdd.union(file_rdd).distinct()
    asociados = rdd.groupByKey().mapValues(list).flatMap(funcion_auxiliar1)
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
        apartado3(sc,lista)