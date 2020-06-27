
import numpy as np
from estructura import *
from distancias import *


#######Espacio de parametros

archivo = 'cociente_raw_uniform_od.csv'
orden = [1,3,0,2] 
factores_origen = [1,0.25,0.25,0.5] #Colocados en orden de los campos originales
distancias_origen = [L_distance_vacios,L_distance_vacios, distancia_levenstein_separando_vacios,L_distance_vacios]
multiplicador = 2/3


#### Preparación parámetros
longitud = len(orden)
factores=[0 for x in range(len(orden))]
for i in range(len(orden)):
    factores[i] = factores_origen[:orden[i]]+factores_origen[orden[i]+1:]
distancias=[0 for x in range(len(orden))]
for i in range(len(orden)):
    distancias[i] = distancias_origen[:orden[i]]+distancias_origen[orden[i]+1:]
for i in range(len(factores)):
    factores[i] = [multiplicador*x for x in factores[i]]
list_of_maps = [seleccionar_map(orden[0])]
list_of_matchs = []
for i in range(len(orden)-1):
    list_of_maps.append(seleccionar_map_intermedio(orden[i+1],orden[i]))
for i in range(len(orden)):
    list_of_matchs.append(seleccionar_match(factores[i],distancias[i]))
last_match = seleccionar_match(factores[0],distancias[0], excluyentes = 2) 


##### Función de preparación del dataset

def mapeo_desde_archivo(x, l = 5):
    datos = x.split(';')
    for i in range(len(datos)):
        if i == l-1:
            datos[i] = set(eval(datos[i]))
        else:
            datos[i] = set([datos[i]])
    return datos



#### Ejecución



result, tiempo = algoritmo(orden,list_of_maps,list_of_matchs,last_match,archivo,mapeo_desde_archivo)

print('Fin: {} s'.format(tiempo))

with open(archivo.split('.')[0]+'_resultado.csv','w') as f:
    for item in result:
        f.write('%s\n' % item)


