import json
import threading
import queue
import matplotlib.pyplot as plt
from kafka import KafkaConsumer

# Cola para almacenar los datos que llegan del consumidor.
data_queue = queue.Queue()

# Configuración del consumidor Kafka.
# Cabe destacar que lo pusimos en un hilo para que no se bloquee el hilo principal de la gráfica.
def kafka_consumer_thread():
    consumer = KafkaConsumer(
        '20461',
        bootstrap_servers = 'lab9.alumchat.xyz:9092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    for message in consumer:
        data_queue.put(message.value)

# Se inicia el hilo del consumidor.
consumer_thread = threading.Thread(target=kafka_consumer_thread)
consumer_thread.daemon = True
consumer_thread.start()

# Arrays para almacenar los datos que se van a graficar.
temperaturas = []
humedades = []
direcciones_viento = []

# Grafica ion para que se actualice en tiempo real.
plt.ion() 
fig, ax = plt.subplots(3, 1, figsize=(10, 8))

# Función para actualizar la gráfica. Se ejecuta en el hilo principal.
def update_plots():
    while True:
        try:
            if not data_queue.empty():
                # Se obtienen los datos de la cola.
                data = data_queue.get()
                data = json.loads(data)

                # Se almacenan los datos en los arrays.
                temperaturas.append(data['temperatura']/100)            # Se divide entre 100 para obtener la temperatura real. Asi se obtiene los decimales.
                humedades.append(data['humedad'])

                # 0 = N, 1 = NE, 2 = E, 3 = SE, 4 = S, 5 = SO, 6 = O, 7 = NO
                dic_viento = ['N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO']
                direcciones_viento.append(dic_viento[data['direccion_viento']])     # Se obtiene la dirección del viento en string.

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
