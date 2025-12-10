#!/bin/bash
# chmod +x startup.sh
# ./startup.sh
echo "🚀 Starting Kafka + Python data pipeline..."

# ----------------------------
# 1) Lancer Docker (Kafka + Zookeeper)
# ----------------------------
echo "🐳 Starting Docker containers..."
docker compose up -d

# ----------------------------
# 2) Créer environnement Python
# ----------------------------
if [ ! -d "venv" ]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
fi

echo "📦 Activating venv and installing dependencies..."
source venv/bin/activate
python3 -m pip install -r requirements.txt

# ----------------------------
# 3) Installer Playwright
# ----------------------------
echo "🌐 Installing Playwright Chromium..."
playwright install chromium

# ----------------------------
# 4) Supprimer les anciens topics Kafka
# ----------------------------

docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic price-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic trade-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic alert-topic
docker exec -it kafka kafka-topics   --bootstrap-server kafka:29092   --delete   --topic article-topic

# ----------------------------
# 4) Creer les topics Kafka
# ----------------------------

echo "📌 Creating Kafka topics..."
docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic price-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic trade-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic alert-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --create \
  --topic article-topic --partitions 1 --replication-factor 1

docker exec -it kafka kafka-topics --bootstrap-server kafka:29092 --list

# ----------------------------
# 5) Lancer les scripts en background
# ----------------------------
echo "📡 Starting price-topic.py in background..."
nohup python3 price-topic.py > price.log 2>&1 &

echo "📰 Starting article-topic.py in background..."
nohup python3 article-topic.py > article.log 2>&1 &

# ----------------------------
# 6) Afficher les derniers logs
# ----------------------------
echo "📊 Last 10 lines of price.log:"
tail -n 10 price.log

echo "📰 Last 10 lines of article.log:"
tail -n 10 article.log

echo "Making the topics available"
nohup python3 ws.py > ws.log 2>&1 &

echo "✅ System started successfully!"



# read the messages

# docker exec -it kafka kafka-console-consumer \
    #--bootstrap-server kafka:29092 \
    #--topic price-topic \
    #--from-beginning

# ps aux | grep ws.py
# lister les process de ws lancer en background
# kill -9 ID en supprimer 1