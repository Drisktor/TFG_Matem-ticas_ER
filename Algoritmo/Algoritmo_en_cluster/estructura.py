from pyspark import SparkContext , SparkConf
import time as t
######## Funciones de estructura

def seleccionar_map(i):
    return lambda x : mapeo_inicial(x,i)

def seleccionar_map_intermedio(i,j):
    return lambda x : mapeo_intermedio(x,i,j)

def mapeo_inicial(x,i):
    key = x[i]
    final_data= []
    final_data.extend(x[:i])
    final_data.extend(x[i+1:])
    return (key), final_data

def mapeo_intermedio(x,i,j):
    antigua_key = set([x[0]])
    if j<i:
        nueva_key = x[1][i-1]
        final_data= []
        final_data.extend(x[1][:j])
        final_data.extend([antigua_key])
        final_data.extend(x[1][j:i-1])
        final_data.extend(x[1][i:])
        return (nueva_key), final_data
    else:
        nueva_key = x[1][i]
        final_data= []
        final_data.extend(x[1][:i])
        final_data.extend(x[1][i+1:j])
        final_data.extend([antigua_key])
        final_data.extend(x[1][j:])
        return (nueva_key), final_data
    
def seleccionar_match(factors,distances, excluyentes = 1):
    return lambda x: match_and_merge(x,factors,distances, excluyentes)

def match_and_merge(x,factors,distances, excluyentes = 1):
    threshold = 1
    return_list = []
    key, iterador = x
    data = list(iterador)
    data.sort(key=lambda x: len(x[3]),reverse=True)
    comprobados = []
    contador = 0
    result=dict()
    i=0
    for element in data[:]:
        seguimos = True
        if i not in comprobados:
            result[contador] = (element)
            while seguimos:
                j=0
                seguimos = False # Esto es para poder cazar todos, seguimos iterando hasta no pegar nada
                for element2 in data[:]:
                    if j not in comprobados and distancia(result[contador],element2,factors,distances, excluyentes) <=  threshold:
                        comprobados.append(j)
                        seguimos = True
                        temp = result[contador]
                        for cont in range(len(element2)):
                            temp[cont] = temp[cont].union(element2[cont])
                        result[contador] = (temp)
                    j+=1
            contador+=1
        i+=1
    for i in range(len(result)):
        return_list.append((key,result[i]))
    return return_list

def separar_keys(x):
    keys = x[0]
    return_list =[]
    for key in list(keys):
        return_list.append(((key),x[1]))
    return return_list



def corregir_ids(x):
    num_list = list(x[1][0])
    maximo = 0
    ID = 0
    for i in range(len(num_list)):
        if num_list[i][1] > maximo:
            maximo = num_list[i][1]
            ID = num_list[i][0]
    return ID, x[1][1],x[1][2],x[0],x[1][3]

def copiar_leading_set(x):
    key = x[0]
    values = x[1].copy()
    values.extend([key])
    return key,values

def orden_final(x):
    values = x[1]
    return_list =[]
    return_list.extend([values[-1]])
    for i in range(len(values)-1):
        return_list.extend([values[i]])
    return return_list

def map_registros(x):
    temp = list(x[-1])
    temp.sort()
    key = tuple(temp)
    final_data= x[:-1]
    return key, final_data

def merge_registros(x):
    key, iterador = x
    data = list(iterador)
    base_data = data[0]
    for element in data[1:]:
        for count in range(len(base_data)):
            base_data[count] = base_data[count].union(element[count])
    base_data.append(set(key))
    return base_data


#### Distancia global


def distancia_para_sets(set1,set2,distance):
    distancia_temp=[1000]
    for element in set1:
            for element2 in set2:
                distancia_temp.append(distance(element,element2))
    return min(distancia_temp)

def distancia(list_of_sets_1,list_of_sets_2, list_of_factors,list_of_distances, excluyentes = 1):
    distancia = 0
    
    for i in range(len(list_of_sets_1)-excluyentes):
        
        distancia = distancia + distancia_para_sets(list_of_sets_1[i],list_of_sets_2[i],list_of_distances[i])*list_of_factors[i]
        
    return distancia


##### Funcion algoritmo

def algoritmo(orden,list_of_maps,list_of_matchs,last_match,archivo,mapeo_preparacion):
    conf = SparkConf().setAppName('Algoritmo ER')
    sc = SparkContext(conf = conf)
    sc.addPyFile('estructura.py')
    sc.addPyFile('distancias.py')
    start = t.time()
    rdd = sc.textFile(archivo).map(mapeo_preparacion)
    rdd_op = rdd
    for i in range(len(orden)):
        rdd_op = rdd_op.map(list_of_maps[i]).flatMap(separar_keys).groupByKey().flatMap(list_of_matchs[i])
    rdd_op = rdd_op.map(seleccionar_map_intermedio(orden[0],orden[i]))
    rdd_op = rdd_op.map(copiar_leading_set).flatMap(separar_keys)
    rdd_op = rdd_op.groupByKey().flatMap(last_match).map(orden_final)
    rdd_op = rdd_op.map(map_registros).groupByKey().map(merge_registros)
    result = rdd_op.collect()
    rdd_op.saveAsTextFile('/user/******/*******')
    end= t.time()
    return result, end-start
