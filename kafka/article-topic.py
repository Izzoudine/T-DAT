from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from collections import OrderedDict
from datetime import datetime
import schedule
from kafka import KafkaProducer
import json


crypto_keywords_extended = list(OrderedDict.fromkeys([
    'BITCOIN','BTC', 'ETHEREUM', 'ETH',
    'DEFI', 'NFT', 'WEB3', 'SOLANA', 'SOL', 'CARDANO', 'ADA',
    'USDT', 'TETHER', 'BNB', 'BINANCE', 'XRP', 'RIPPLE', 'USDC',
    'DOGE', 'DOGECOIN', 'TRX', 'TRON', 'DOT', 'POLKADOT', 'MATIC', 'POLYGON',
    'LTC', 'LITECOIN', 'SHIB', 'SHIBA', 'AVAX', 'AVALANCHE', 'LINK', 'CHAINLINK',
    'ATOM', 'COSMOS', 'XLM', 'STELLAR', 'BCH', 'BITCOIN CASH', 'FIL', 'FILECOIN',
    'APT', 'APTOS', 'ARB', 'ARBITRUM', 'OP', 'OPTIMISM', 'NEAR', 'NEAR PROTOCOL',
    'ALGO', 'ALGORAND', 'VET', 'VECHAIN', 'ICP', 'INTERNET COMPUTER', 'HBAR', 'HEDERA',
    'QNT', 'QUANT', 'SAND', 'THE SANDBOX', 'MANA', 'DECENTRALAND', 'AAVE', 'EGLD', 'ELROND',
    'XTZ', 'TEZOS', 'GRT', 'THE GRAPH', 'FTM', 'FANTOM', 'STX', 'STACKS', 'RNDR', 'RENDER',
    'LDO', 'LIDO', 'CRV', 'CURVE', 'DYDX', 'GMX', 'SUI', 'PEPE', 'TWT', 'TRUST WALLET',
    'CAKE', 'PANCAKESWAP', 'ONE', 'HARMONY', 'KAVA', 'KLAY', 'KLAYTN', 'FLOW', 'GALA',
    'ENJ', 'ENJIN', 'CHZ', 'CHILIZ', 'AXS', 'AXIE', 'SUSHI', 'SUSHISWAP', 'COMP', 'COMPUND',
    'YFI', 'YEARN', 'SNX', 'SYNTHETIX', 'MKR', 'MAKER', 'ZIL', 'ZILLIQA', 'BAT', 'BASIC ATTENTION TOKEN',
    'OMG', 'OMG NETWORK', 'DASH', 'ZEC', 'ZCASH', 'QTUM', 'ICX', 'ICON', 'ONT', 'ONTOLOGY',
    'RVN', 'RAVENCOIN', 'SC', 'SIACOIN', 'ZEN', 'HORIZEN', 'NANO', 'WAVES', 'DCR', 'DECRED',
    'LSK', 'LISK', 'STEEM', 'BTT', 'BITTORRENT', 'HOT', 'HOLO', 'CEL', 'CELSIUS', 'KSM', 'KUSAMA',
    'SRM', 'SERUM', 'CRO', 'CRONOS', 'FTT', 'FTX', 'LUNA', 'TERRA', 'UST', 'TERRAUSD',
    'XEM', 'NEM', 'DGB', 'DIGIBYTE', 'SXP', 'SOLAR', 'BUSD', 'BINANCE USD', 'PAX', 'PAXOS',
    'USDP', 'PAX DOLLAR', 'TUSD', 'TRUEUSD', 'GUSD', 'GEMINI DOLLAR', 'FRAX',
    'MINA', 'MINA PROTOCOL', 'RUNE', 'THORCHAIN', 'CELO', 'ANKR',
    'CKB', 'NERVOS', 'RSR', 'RESERVE RIGHTS', 'LRC', 'LOOPRING', 'BNT', 'BANCOR',
    'BAL', 'BALANCER', 'CVC', 'CIVIC', 'STORJ', 'OCEAN', 'OCEAN PROTOCOL',
    'BAND', 'BAND PROTOCOL', 'API3', 'FET', 'FETCH.AI', 'AGIX', 'SINGULARITYNET',
    'INJ', 'INJECTIVE', 'FLUX', 'GLMR', 'MOONBEAM', 'MOVR', 'MOONRIVER',
    'XDC', 'XDC NETWORK', 'SFP', 'SAFEPAL', 'CTSI', 'CARTESI', 'DODO',
    'ALICE', 'MYNEIGHBORALICE', 'BAKE', 'BAKERYTOKEN', 'TOMO', 'TOMOCHAIN',
    'PERP', 'PERPETUAL PROTOCOL', 'MDX', 'BEL', 'BELT', 'FOR.TUBE',
    'LIT', 'LITENTRY', 'PHA', 'PHALA', 'BTS', 'BITSHARES', 'DOCK', 'MLN',
    'ENZYME', 'RLC', 'IEXEC', 'WNXM', 'WRAPPED NEXUS MUTUAL', 'REP', 'AUGUR',
    'ANT', 'ARAGON', 'NMR', 'NUMERAIRE', 'COTI', 'POWR', 'POWER LEDGER',
    'FUN', 'FUNFAIR', 'MITH', 'MITHRIL', 'DATA', 'STREAMR', 'DENT', 'STMX',
    'STORMX', 'MTL', 'METAL', 'AMB', 'AMBROSUS', 'BLZ', 'BLAZAR', 'QKC',
    'QUARKCHAIN', 'CHR', 'CHROMIA', 'AKRO', 'AKROPOLIS', 'MIR', 'MIRROR PROTOCOL',
    'ANC', 'ANCHOR PROTOCOL', 'ORCA', 'RAY', 'RAYDIUM', 'SLIM', 'SOLMINTER',
    'FIDA', 'BONFIDIA', 'MAPS', 'MAPS.ME', 'OXY', 'OXYGEN', 'COPE', 'STEP',
    'STEP FINANCE', 'MEDIA NETWORK', 'SAMO', 'SAMOYEDCOIN', 'POLIS',
    'STAR ATLAS DAO', 'ATLAS', 'STAR ATLAS', 'GENE', 'GENOPETS', 'DRIFT',
    'DRIFT PROTOCOL'
]))

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

def extract(content, open_balise, close_balise):
    print("Inside the clean function")
    print(content.find(open_balise))
    start = content.find(open_balise) + len(open_balise)
    end = content.find(close_balise)
    txt = content[start:end]
    return txt

def scrapper(url, website, type = 0):
    with sync_playwright() as p:
        # lancer sans interface graphique
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # simuler la visite d'un utilisateur
        page.set_extra_http_headers({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        page.goto(url, wait_until='domcontentloaded', timeout=30000)
        page.wait_for_timeout(500)

        html = page.content()
        soup = BeautifulSoup(html, 'lxml' if type == 0 else 'lxml-xml')
        items = soup.find_all('item', '')

        for item in items:
            extracted_text = extract(str(item.find("title")), "<title>", "</title>")
            extracted_description = extract(str(item.find("description").find("p")), "<p>", "</p>") if type == 0 else extract(str(item.find("description")), "<description>", "</description>")
            extracted_link = extract(str(item.find("guid")), "<guid>", "</guid>") if type == 0 else extract(str(item.find("link")), "<link>", "</link>")

            current_time = time.time()
            time_scrapped = time.ctime(current_time)

            text_upper = extracted_description.upper()
            cryptos = [crypto for crypto in crypto_keywords_extended if crypto in text_upper]
            
            sentiment_scores = sentiment_analyzer.polarity_scores(extracted_description)
            compound = sentiment_scores['compound']

            if compound >= 0.05:
                label = 'positive'
            elif compound <= -0.05:
                label = 'negative'
            else:
                label = 'neutral' 

            print(label)

            article = ({
                             "id" : f"{website}_{int(datetime.now().timestamp())}",
                             "title":extracted_text, 
                             "description":extracted_description, 
                             "link":extracted_link,
                             "website": website, 
                             "time":time_scrapped, 
                             "cryptos":cryptos,
                             "sentiment": {
                                'score': round(compound, 3),
                                'label': label,
                                'confidence': round(abs(compound), 3)
                             }})
            # envoie asynchrone de l'arctile
            producer.send("article-topic", article)
            time.sleep(1)
        browser.close()    
        
        # forcer envoie immediat de tout ce qui reste en attente
        producer.flush()


def job():
    
    scrapper("https://cointelegraph.com/rss", "cointelegraph.com")[6]
    scrapper("https://coindesk.com/arc/outboundfeeds/rss", "coindesk.com" ,type=1)[6]
    scrapper("https://decrypt.co/feed", "decrypt.co" ,type=1)[6]


def main():
    
    job()
    # toutes les 24 heures
    schedule.every(1).days.do(job)

    while True:
        schedule.run_pending()
        # verifie toutes les heures
        time.sleep(60)



