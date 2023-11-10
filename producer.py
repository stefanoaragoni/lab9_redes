from kafka import KafkaProducer
import json
import time
import random
from generator import generar_datos

# Configuración del productor Kafka
producer = KafkaProducer(
    bootstrap_servers='lab9.alumchat.xyz:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = '20461'

# Ciclo para enviar datos cada 15 a 30 segundos
try:
    while True:
        data = generar_datos()
        producer.send(topic, value=data)
        print(f"Sent data: {data}")

        time.sleep(random.uniform(15, 30))

except KeyboardInterrupt:
    print("Deteniendo el productor...")

finally:
    producer.flush()
    producer.close()
