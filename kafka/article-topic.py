import requests # Beaucoup plus léger que Playwright pour du RSS
from bs4 import BeautifulSoup
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from datetime import datetime
import schedule
from kafka import KafkaProducer
import json
import hashlib
import re


NARRATIVE_KEYWORDS = {
    "SECURITY": [
        "hack", "hacked", "exploit", "stolen", "breach", "attacker", 
        "vulnerability", "phishing", "scam", "rug pull", "drain"
    ],
    "REGULATION": [
        "sec", "regulation", "regulators", "lawsuit", "sued", "ban", 
        "illegal", "compliant", "tax", "gensler", "government", "court","doj", "guilty", "pleads", "convicted", "sentence", "jail", "prison", "fine", "penalty"
    ],
    "LISTING_EXCHANGE": [
        "listing", "listed", "delist", "exchange", "binance", "coinbase", 
        "kraken", "withdrawals suspended", "liquidity"
    ],
    "MACRO_ECONOMY": [
        "inflation", "fed", "fomc", "interest rate", "cpi", "powell", 
        "recession", "economy", "dollar", "stocks", "nasdaq"
    ],
    "TECH_UPGRADE": [
        "mainnet", "testnet", "upgrade", "fork", "hard fork", "launch", 
        "release", "beta", "roadmap", "partnership", "integrate"
    ],
    "WHALE_MARKET": [
        "whale", "transfer", "wallet", "accumulation", "dump", 
        "all-time high", "ath", "resistance", "support", "bull run"
    ]
}

MASTER_CRYPTO_MAP = {
    # --- LES GÉANTS (L1 & Stores of Value) ---
    "BTC": ["bitcoin", "btc", "satoshi", "xbt"],
    "ETH": ["ethereum", "ether", "eth", "vitalik"], # 'Vitalik' souvent synonyme d'ETH dans les titres
    "SOL": ["solana", "sol"],
    "BNB": ["binance coin", "bnb", "bsc", "binance smart chain"],
    "XRP": ["ripple", "xrp", "xrp ledger"],
    "ADA": ["cardano", "ada"],
    "AVAX": ["avalanche", "avax"],
    "TRX": ["tron", "trx", "justin sun"],
    "DOT": ["polkadot", "dot"], # Attention: 'dot' est un mot courant, voir section regex
    "MATIC": ["polygon", "matic", "pol"], # POL est le nouveau ticker, mais mot risqué
    "TON": ["toncoin", "ton", "the open network"],
    "KAS": ["kaspa", "kas"],

    # --- STABLECOINS (Important pour détecter les flux) ---
    "USDT": ["tether", "usdt"],
    "USDC": ["usd coin", "usdc", "circle"],
    "DAI": ["dai", "makerdao"],
    "FDUSD": ["first digital usd", "fdusd"],

    # --- DEFI & INFRA ---
    "LINK": ["chainlink", "link"], # 'link' est risqué
    "UNI": ["uniswap", "uni"],
    "AAVE": ["aave"],
    "LDO": ["lido", "ldo"],
    "ARB": ["arbitrum", "arb"],
    "OP": ["optimism", "op"],
    "TIA": ["celestia", "tia"],
    "INJ": ["injective", "inj"],
    "RUNE": ["thorchain", "rune"],
    
    # --- AI & BIG DATA (Tendance actuelle) ---
    "FET": ["fetch.ai", "fetch", "fet", "asi"],
    "RNDR": ["render", "rndr", "render token"],
    "NEAR": ["near protocol", "near"], # 'near' est très risqué (préposition)
    "WLD": ["worldcoin", "wld", "sam altman"],
    "TAO": ["bittensor", "tao"],

    # --- MEMECOINS (Le plus de volatilité) ---
    "DOGE": ["dogecoin", "doge"],
    "SHIB": ["shiba inu", "shib", "shibarium"],
    "PEPE": ["pepe", "pepecoin"],
    "WIF": ["dogwifhat", "wif"],
    "BONK": ["bonk"],
    "FLOKI": ["floki", "floki inu"],
    "BRETT": ["brett"],
    "POPCAT": ["popcat"],
}

# Liste noire stricte : Mots qui, s'ils sont trouvés seuls en minuscules, 
RISKY_KEYWORDS = {
    "one", "link", "dot", "near", "uni", "bond", "gas", "pol", "rose", "trump", "cat", "baby"
}

def determine_narrative(text):
    """
    Analyse le texte et retourne la categorie dominante
    """

    text = text.lower()
    scores = {category: 0 for category in NARRATIVE_KEYWORDS}

    for category, keywords in NARRATIVE_KEYWORDS.items():
        for word in keywords:
            # on compte le nombre d'occurrences
            count = text.count(word)
            if count > 0:
                scores[category] += count

    # la categorie avec le score le plus eleve            
    best_category = max(scores, key=scores.get)

    if scores[best_category] == 0:
        return "GENERAL_NEWS"

    return best_category            
            

def build_search_pattern(master_map):
    keyword_to_ticker = {}
    all_keywords = []

    for ticker, synonyms in master_map.items():
        for syn in synonyms:
            syn_lower = syn.lower()
            keyword_to_ticker[syn_lower] = ticker
            all_keywords.append(re.escape(syn_lower))
    
    pattern_string = r'\b(' + '|'.join(all_keywords) + r')\b'
    regex = re.compile(pattern_string, re.IGNORECASE)
    
    return keyword_to_ticker, regex
KEYWORD_TO_TICKER, CRYPTO_REGEX = build_search_pattern(MASTER_CRYPTO_MAP)

def extract_cryptos_from_text(text, description):
    """
    Extrait les cryptos uniques d'un texte complet.
    Gère les faux positifs basiques via la liste noire.
    """
    content = (text + " " + description)
    found_tickers = set()
    
    # 1. Recherche via Regex (Très rapide)
    matches = CRYPTO_REGEX.findall(content)
    
    for match in matches:
        word = match.lower()
        ticker = KEYWORD_TO_TICKER[word]
        
        # 2. Filtrage des mots risqués (Risk Management)
        if word in RISKY_KEYWORDS:
            if not re.search(r'\b' + re.escape(word.upper()) + r'\b', content):
                continue

        found_tickers.add(ticker)
        
    return list(found_tickers)

sentiment_analyzer = SentimentIntensityAnalyzer()
producer = KafkaProducer(
            bootstrap_servers='20.199.136.163:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            # le leader confirme l'ecriture avant de continuer
            acks=1,
             # nombre de retry
            retries=3,
            # envoie 5 messages en paralleles
            max_in_flight_requests_per_connection=5
        )


# --- TES CONFIGS (MAPS, ETC) RESTENT ICI ---
# (Je ne les remets pas pour garder la réponse courte, garde tes dictionnaires)
# ...

sentiment_analyzer = SentimentIntensityAnalyzer()

# Connexion Kafka sécurisée (évite le crash si Kafka n'est pas prêt au démarrage)
producer = None
try:
    producer = KafkaProducer(
        bootstrap_servers='20.199.136.163:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks=1,
        retries=3
    )
except Exception as e:
    print(f"⚠️ Warning: Kafka non connecté ({e})")

def clean_html(raw_html):
    """Retire les balises HTML pour ne garder que le texte pur pour VADER"""
    if not raw_html: return ""
    soup = BeautifulSoup(raw_html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

def scrapper(url, website):
    print(f"🚀 Scraping {website} via Requests...")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        # 1. Utilisation de requests (Rapide & Léger)
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status() # Lève une erreur si 404/500
        
        # 2. Parsing XML robuste avec BeautifulSoup
        # 'xml' est le meilleur parser pour les RSS
        soup = BeautifulSoup(response.content, 'xml') 
        items = soup.find_all('item')

        if not items:
            print(f"⚠️ Aucun item trouvé pour {website}")
            return

        for item in items:
            # 3. Extraction Robuste (Pas de slicing manuel !)
            title_tag = item.find("title")
            desc_tag = item.find("description")
            # Certains RSS utilisent <guid> d'autres <link>
            link_tag = item.find("guid") 
            if not link_tag:
               link_tag = item.find("guid")

            # Sécurité : on saute si manque d'info critique
            if not title_tag or not link_tag: continue

            extracted_title = title_tag.get_text(strip=True)
            extracted_link = link_tag.get_text(strip=True)
            raw_desc = desc_tag.get_text(strip=True) if desc_tag else ""

            # 4. Nettoyage HTML pour l'IA
            clean_desc = clean_html(raw_desc)
            
            # Filtre anti-bruit (vidéos, descriptions vides)
            if "/videos/" in extracted_link or len(clean_desc) < 20:
                continue

            # Création de l'ID unique
            article_id = hashlib.md5(extracted_link.encode('utf-8')).hexdigest()
            
            # Analyse IA
            full_text = f"{extracted_title} {clean_desc}"
            cryptos = extract_cryptos_from_text(full_text, "") # Ta fonction adaptée
            narrative = determine_narrative(clean_desc)
            
            scores = sentiment_analyzer.polarity_scores(clean_desc)
            compound = scores['compound']
            label = 'positive' if compound >= 0.05 else 'negative' if compound <= -0.05 else 'neutral'

            article = {
                "id": article_id,
                "title": extracted_title,
                "description": clean_desc, # On garde la version propre
                "link": extracted_link,
                "website": website,
                "time": datetime.utcnow().isoformat(),
                "cryptos": cryptos,
                "narrative": narrative,
                "sentiment": {
                    'score': compound,
                    'label': label
                }
            }
            print(article)

            if producer:
                producer.send("article-topic", article)
            
            # Pas de sleep(1) ici ! On veut aller vite.

        if producer:
            producer.flush()
            print(f"✅ {len(items)} articles traités pour {website}")

    except Exception as e:
        print(f"❌ Erreur sur {website}: {e}")

def job():
    # Plus besoin de paramètre 'type', le parser 'xml' gère tout
    scrapper("https://cointelegraph.com/rss", "cointelegraph.com")
    scrapper("https://coindesk.com/arc/outboundfeeds/rss", "coindesk.com")
    scrapper("https://decrypt.co/feed", "decrypt.co")

def main():
    job()
    schedule.every(5).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(10)

if __name__ == "__main__":
    main()