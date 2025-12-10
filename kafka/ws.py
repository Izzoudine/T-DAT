import asyncio
import json
from kafka import KafkaConsumer
import websockets

# --------------------
# CONFIG Kafka
# --------------------
KAFKA_BOOTSTRAP = 'kafka:29092'  # adresse de ton broker Kafka
KAFKA_TOPICS = ['price-topic', 'trade-topic', 'alert-topic', 'article-topic']  # tous les topics que tu veux exposer

consumer = KafkaConsumer(
    *KAFKA_TOPICS,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='latest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# --------------------
# Gestion clients WebSocket
# --------------------
clients = set()

async def register(ws):
    clients.add(ws)
    try:
        await ws.wait_closed()
    finally:
        clients.remove(ws)

async def broadcast(message):
    if clients:
        await asyncio.gather(*[client.send(message) for client in clients])

async def ws_handler(ws):
    await register(ws)

# --------------------
# Boucle Kafka
# --------------------
async def kafka_loop():
    loop = asyncio.get_event_loop()
    for msg in consumer:
        data = msg.value
        # Diffuser directement aux clients WS
        loop.create_task(broadcast(json.dumps(data)))

# --------------------
# Lancer le serveur WS et Kafka
# --------------------
async def main():
    server = await websockets.serve(ws_handler, "0.0.0.0", 8000)
    print("WebSocket server running on ws://0.0.0.0:8000")
    await kafka_loop()  # boucle bloquante Kafka

asyncio.run(main())
