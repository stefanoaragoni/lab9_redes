import json
import random
import time
from kafka import KafkaProducer

# Dominio: lab9.alumchat.xyz
# IP: 157.245.244.105
# Puerto: 9092

producer = KafkaProducer(bootstrap_servers='localhost:9092')

def enviar_datos_kafka(datos_json):
    # Envía los datos al topic 'estacion_meteorologica'.
    producer.send('estacion_meteorologica', datos_json.encode('utf-8'))

# Simulamos la generación y envío de datos cada 20 segundos (como ejemplo).
while True:
    datos = generar_datos_meteorologicos()
    enviar_datos_kafka(datos)
    time.sleep(20)
