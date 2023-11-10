import json
import random
import time
from kafka import KafkaProducer

def generar_datos():

    temperatura = round(random.gauss(50, 10), 2)                        # Temperatura entre 0 y 100. Float con 2 decimales.
    humedad = int(random.gauss(50, 10))                                 # Humedad entre 0 y 100. Entero.
    
    direcciones_viento = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO']   # Direcciones del viento. Lista de strings.
    direccion_viento = random.choice(direcciones_viento)                # Dirección del viento aleatoria.
    
    # Almacenamos los datos en formato JSON.
    datos_json = json.dumps({
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion_viento
    })
    
    return datos_json


def generar_datos_3bytes():

    temperatura = round(random.gauss(50, 10), 2)                        # Temperatura entre 0 y 100. Float con 2 decimales.
    
    # Temperatura 000DD, donde 000 es la parte entera y DD la parte decimal. De tal manera, se envía un entero
    # Por tal razón, multiplicamos por 100 y convertimos a entero.
    temperatura = int(temperatura * 100)

    # Ya que la temperatura máxima 10,000, se necesitan 14 bits para representarla en binario.

    # ------------------------------

    humedad = int(random.gauss(50, 10))                                 # Humedad entre 0 y 100. Entero.

    # Para representar 100 en binario, se necesitan 7 bits.

    # ------------------------------

    # Direcciones de string ahora es un int de 0 a 7.
    # Donde 0 = N, 1 = NE, 2 = E, 3 = SE, 4 = S, 5 = SO, 6 = O, 7 = NO
    direccion_viento = random.randint(0, 7)                             # Dirección del viento aleatoria. Int de 0 a 7. 

    # Para representar 7 en binario, se necesitan 3 bits.

    # ------------------------------

    # En total, se tienen 14 + 7 + 3 = 24 bits. Esto es igual a 3 bytes.

    # Almacenamos los datos en formato JSON.
    datos_json = json.dumps({
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion_viento
    })
    
    return datos_json