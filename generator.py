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
