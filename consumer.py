import json
import threading
import queue
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Configuración del consumidor Kafka
bootstrap_servers = 'lab9.alumchat.xyz:9092'
topic_name = '20461'
group_id = 'my-group'

# Cola para la comunicación entre el hilo de consumidor y el hilo principal
data_queue = queue.Queue()

# Función para el hilo de consumidor que recibe los mensajes de Kafka y los coloca en la cola
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        topic_name,
        group_id=group_id,
        bootstrap_servers=bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data_queue.put(message.value)

# Inicialización y comienzo del hilo de consumidor
consumer_thread = threading.Thread(target=kafka_consumer_thread)
consumer_thread.daemon = True
consumer_thread.start()

# Inicialización de las listas para los datos
temperaturas = []
humedades = []
direcciones_viento = []

# Configuración inicial de la figura de matplotlib
plt.ion()  # Activa el modo interactivo
fig, ax = plt.subplots(3, 1, figsize=(10, 8))

# Función para actualizar las gráficas
def update_plots():
    while True:
        try:
            # Obtener datos de la cola
            if not data_queue.empty():
                data = data_queue.get()

                # Asegúrate de deserializar el string JSON a un diccionario de Python
                data = json.loads(data)

                # Ahora puedes acceder a los datos como esperabas
                temperaturas.append(data['temperatura'])
                humedades.append(data['humedad'])
                direcciones_viento.append(data['direccion_viento'])

                # Actualiza la gráfica de temperatura
                ax[0].cla()
                ax[0].plot(temperaturas, label='Temperatura')
                ax[0].set_title('Temperatura')
                ax[0].legend()

                # Actualiza la gráfica de humedad
                ax[1].cla()
                ax[1].plot(humedades, label='Humedad')
                ax[1].set_title('Humedad')
                ax[1].legend()

                # Actualiza la gráfica de dirección del viento
                ax[2].cla()
                ax[2].plot(direcciones_viento, label='Dirección del Viento')
                ax[2].set_title('Dirección del Viento')
                ax[2].legend()

                # Redibuja las gráficas
                plt.draw()

            plt.pause(0.1)  # Pausa breve para que se actualice la GUI

        except KeyboardInterrupt:
            print("Deteniendo el consumidor y cerrando la gráfica...")
            plt.close(fig)
            break

# Comienza la actualización de las gráficas
update_plots()
