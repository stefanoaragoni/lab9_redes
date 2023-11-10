import json
import threading
import queue
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Configuración del consumidor Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic_name = '20461'
group_id = 'my-group'

data_queue = queue.Queue()

def kafka_consumer_thread():
    consumer = KafkaConsumer(
        topic_name,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data_queue.put(message.value)

consumer_thread = threading.Thread(target=kafka_consumer_thread)
consumer_thread.daemon = True
consumer_thread.start()

temperaturas = []
humedades = []
direcciones_viento = []

plt.ion() 
fig, ax = plt.subplots(3, 1, figsize=(10, 8))

def update_plots():
    while True:
        try:
            if not data_queue.empty():
                data = data_queue.get()
                data = json.loads(data)

                temperaturas.append(data['temperatura'])
                humedades.append(data['humedad'])
                direcciones_viento.append(data['direccion_viento'])

                ax[0].cla()
                ax[0].plot(temperaturas, label='Temperatura')
                ax[0].set_title('Temperatura')
                ax[0].legend()

                ax[1].cla()
                ax[1].plot(humedades, label='Humedad')
                ax[1].set_title('Humedad')
                ax[1].legend()

                ax[2].cla()
                ax[2].plot(direcciones_viento, label='Dirección del Viento')
                ax[2].set_title('Dirección del Viento')
                ax[2].legend()

                plt.draw()

            plt.pause(0.1)  

        except KeyboardInterrupt:
            print("Deteniendo el consumidor y cerrando la gráfica...")
            plt.close(fig)
            break

update_plots()
