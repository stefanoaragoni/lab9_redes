from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
from collections import deque
from threading import Thread
import time

# Función para procesar los mensajes recibidos
def procesar_mensaje(mensaje):
    return json.loads(mensaje.value)

# Función para graficar los datos en vivo
def plot_all_data(all_temp, all_hume, all_wind):
    plt.clf()
    plt.subplot(3, 1, 1)
    plt.plot(all_temp, label='Temperatura')
    plt.title('Temperatura')
    plt.legend()

    plt.subplot(3, 1, 2)
    plt.plot(all_hume, label='Humedad')
    plt.title('Humedad')
    plt.legend()

    plt.subplot(3, 1, 3)
    plt.plot(all_wind, label='Dirección del viento')
    plt.title('Dirección del Viento')
    plt.legend()

    plt.tight_layout()
    plt.draw()

# Inicializar listas para almacenar los datos
all_temp = deque(maxlen=50)  # Almacenará las últimas 50 temperaturas
all_hume = deque(maxlen=50)  # Almacenará las últimas 50 humedades
all_wind = deque(maxlen=50)  # Almacenará las últimas 50 direcciones del viento

# Configuración del consumidor Kafka
consumer = KafkaConsumer(
    '20461',  # Tu número de carné como nombre del topic
    group_id='group1',
    bootstrap_servers='lab9.alumchat.xyz:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Configurar la gráfica
plt.ion()
fig = plt.figure(figsize=(10, 7))

# Función para consumir y graficar datos
def consume_and_plot():
    for mensaje in consumer:
        print(mensaje)
        payload = procesar_mensaje(mensaje)
        all_temp.append(payload['temperatura'])
        all_hume.append(payload['humedad'])
        all_wind.append(payload['direccion_viento'])

        plot_all_data(all_temp, all_hume, all_wind)
        plt.pause(0.1)  # Pausa con tiempo suficiente para que se actualice la gráfica

# Iniciar el proceso de consumo y graficación en un hilo aparte
thread = Thread(target=consume_and_plot)
thread.start()

# Mantener el script corriendo para que el hilo no muera
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("Deteniendo el consumidor...")
finally:
    consumer.close()
    plt.close(fig)
