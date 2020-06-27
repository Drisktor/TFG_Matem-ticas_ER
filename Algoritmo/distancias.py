
from Levenshtein import distance as L_distance

######Distancias definidas

def L_distance_vacios(element1,element2):
    if element1 == '' or element2 == '':
        return 1
    else:
        return L_distance(element1,element2)

def distancia_levenstein_separando_vacios(element1,element2):
    set1= set(element1.split(' '))
    set2= set(element2.split(' '))
    distancia_temp = [1000]
    if '' in set1 or '' in set2:
        distancia_temp.append(1)
    for element in list(set1):
            for element_2 in list(set2):
                distancia_temp.append(L_distance_vacios(element,element_2))
    return min(distancia_temp)


        

