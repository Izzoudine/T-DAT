import asyncio
import json
import websockets
from aiokafka import AIOKafkaConsumer

# --------------------
# CONFIGURATION
# --------------------
# On utilise localhost:29092 car le script tourne sur la même VM que Docker.
# On utilise le listener INTERNAL qui est mappé sur le port 29092 du host.
KAFKA_BOOTSTRAP = 'localhost:29092' 
KAFKA_TOPICS = ['price-topic', 'trade-topic', 'alert-topic', 'article-topic']
WS_PORT = 8000

# Ensemble des clients connectés
clients = set()

async def register(ws):
    """Enregistre un nouveau client WebSocket."""
    clients.add(ws)
    print(f"➕ Nouveau client connecté. Total: {len(clients)}")
    try:
        await ws.wait_closed()
    finally:
        clients.remove(ws)
        print(f"➖ Client déconnecté. Total: {len(clients)}")

async def broadcast(message):
    """Envoie le message à tous les clients connectés."""
    if clients:
        # On envoie à tous les clients en parallèle sans attendre
        await asyncio.gather(*[client.send(message) for client in clients], return_exceptions=True)

async def consume_kafka():
    """Consomme Kafka de manière asynchrone."""
    consumer = AIOKafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset='latest',
        # Pas besoin de value_deserializer complexe ici, on renvoie du JSON brut ou on décode juste le texte
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    print(f"⏳ Connexion à Kafka ({KAFKA_BOOTSTRAP}) en cours...")
    await consumer.start()
    print("✅ Connecté à Kafka ! En attente de messages...")

    try:
        async for msg in consumer:
            data = msg.value
            # Ajout du topic dans la donnée pour que le client sache d'où ça vient
            payload = json.dumps({
                "topic": msg.topic,
                "data": data
            })
            
            # Diffusion aux WebSockets
            await broadcast(payload)
    except Exception as e:
        print(f"❌ Erreur Kafka: {e}")
    finally:
        await consumer.stop()

async def main():
    # 1. Démarrer le serveur WebSocket
    async with websockets.serve(register, "0.0.0.0", WS_PORT):
        print(f"🚀 Serveur WebSocket démarré sur ws://0.0.0.0:{WS_PORT}")
        
        # 2. Démarrer la consommation Kafka en parallèle
        await consume_kafka()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Arrêt du service.")