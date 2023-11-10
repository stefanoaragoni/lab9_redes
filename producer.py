import json
import random
import time

def generar_datos():
    temperatura = random.gauss(50, 10)      # Temperatura e
    humedad = random.gauss(50, 10)
    
    # Genera un valor aleatorio para la dirección del viento entre 0 y 360 grados.
    direccion_viento = random.randint(0, 360)
    
    # Empaqueta los datos en un formato JSON.
    datos_json = json.dumps({
        'temperatura': temperatura,
        'humedad': humedad,
        'direccion_viento': direccion_viento
    })
    
    return datos_json

# Simulamos la generación de datos cada 20 segundos (como ejemplo).
while True:
    datos = generar_datos()
    print(datos)
    time.sleep(20) # Espera 20 segundos antes de generar nuevos datos.
