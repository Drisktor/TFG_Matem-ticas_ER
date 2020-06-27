from subprocess import call
import pandas as pd

#extensiones = [1000,2500,6250,11100,19400,23400] -> Son extensiones usadas en otras ejecuciones del script
#extensiones = ['mult_x_16', 'mult_x_19','mult_x_22']
extensiones = ['mult_x_3']
sin_extension = False

for i  in extensiones:
    call("rm tiempos.txt", shell = True)
    for j in range(5):
        call("python3 Algoritmo.py /user/*******/*******/cociente_raw_uniform_od_"+str(i)+".csv",shell=True)
        call("hdfs dfs -rm -r /user/*******/*******",shell=True)
    df = pd.read_csv('tiempos.txt', header = None)
    call("echo El tiempo para extension {} es {}. El tiempo con los 4 ultimos es {}  >> tiempo_final.txt".format(i,df.mean()[0],df[1:].mean()[0]),shell = True)

if sin_extension:
    call("rm tiempos.txt", shell = True)
    for j in range(5):
        call("python3 Algoritmo.py /user/*******/*******/cociente_raw_uniform_od.csv",shell=True)
        call("hdfs dfs -rm -r /user/********/******",shell=True)
    df = pd.read_csv('tiempos.txt', header = None)
    call("echo El tiempo para extension nula es {}. El tiempo con los 4 ultimos es {}  >> tiempo_final.txt".format(df.mean()[0],df[1:].mean()[0]),shell = True)
                    
        
        

