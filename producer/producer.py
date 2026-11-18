#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SahlPay.ma - generateur d'evenements de transactions (portefeuille mobile).

Trois modes :
  --mode merchants : genere le referentiel statique data/merchants.json (300 lignes JSON)
  --mode file      : genere un historique JSON Lines (ex. transactions_history.jsonl)
  --mode kafka     : publie les evenements en temps reel dans le topic Kafka "transactions"

Chaque evenement correspond a un changement d'etat d'une transaction :
INITIATED -> AUTHORIZED -> COMPLETED | REJECTED, puis eventuellement REVERSED.

Cas reels simules volontairement :
  * ~5 % d'evenements en retard (jusqu'a 15 minutes, telephone hors reseau) ;
  * ~2 % de doublons (meme tx_id et meme status) ;
  * scenarios de fraude injectes (appareils partages, transferts circulaires A->B->C->A,
    comptes collecteurs recevant de nombreux petits virements).
Les entites frauduleuses dependent uniquement de --seed : elles sont identiques en mode
file et en mode kafka, et sont ecrites dans data/fraud_ground_truth.json (verite terrain
utilisee en Partie 6 pour mesurer le rappel et les faux positifs).

Exemples :
  python producer.py --mode merchants
  python producer.py --mode file --n 50000
  python producer.py --mode kafka --rate 20
  python producer.py --mode kafka --rate 20 --late-demo-every 60
"""
import argparse
import heapq
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA_DIR = os.path.normpath(os.path.join(HERE, "..", "data"))

# --------------------------------------------------------------------------------------
# Parametres metier
# --------------------------------------------------------------------------------------
CITIES = {
    # ville: (poids, latitude, longitude, multiplicateur du taux de rejet)
    "Casablanca": (30, 33.5731, -7.5898, 1.0),
    "Rabat": (14, 34.0209, -6.8416, 0.8),
    "Marrakech": (11, 31.6295, -7.9811, 1.1),
    "Fes": (9, 34.0181, -5.0078, 1.0),
    "Tanger": (9, 35.7595, -5.8340, 1.2),
    "Agadir": (7, 30.4278, -9.5981, 0.9),
    "Meknes": (6, 33.8935, -5.5473, 1.0),
    "Oujda": (5, 34.6814, -1.9086, 1.6),
    "Kenitra": (5, 34.2610, -6.5802, 1.0),
    "Tetouan": (4, 35.5889, -5.3626, 1.1),
}

CATEGORIES = {
    # categorie: (poids, montant median MAD, sigma log-normal, prefixes de noms)
    "epicerie": (30, 85, 0.7, ["Epicerie", "Hanout", "Superette"]),
    "carburant": (12, 300, 0.5, ["Station Service", "Relais Carburant"]),
    "telecom": (12, 50, 0.6, ["Recharge Telecom", "Teleboutique"]),
    "restauration": (16, 120, 0.6, ["Cafe", "Snack", "Restaurant"]),
    "pharmacie": (10, 140, 0.7, ["Pharmacie"]),
    "e-commerce": (10, 450, 0.9, ["Boutique en ligne", "Shop"]),
    "habillement": (6, 380, 0.6, ["Boutique", "Mode"]),
    "transport": (4, 60, 0.5, ["Taxi", "Transport"]),
}

NAME_SUFFIXES = [
    "Al Amal", "Atlas", "Annour", "Assalam", "Al Baraka", "Ennasr", "Al Fath", "Zitoun",
    "Al Andalous", "Riad", "Oasis", "Sahara", "Majorelle", "Bouregreg", "Chellah",
    "Ifrane", "Toubkal", "Argana", "Al Wafa", "Essaada", "Al Massira", "Anfa", "Agdal",
    "Hay Riad", "Maarif", "Gueliz", "Medina", "Kasbah", "Souissi", "Al Hoceima",
]

N_WALLETS = 5000
WALLET_ID_START = 10000           # W-10000 ... W-14999
P2P_SHARE = 0.25                  # part des transactions P2P
LATE_SHARE = 0.05                 # ~5 % d'evenements en retard
DUP_SHARE = 0.02                  # ~2 % de doublons
MAX_LATE_MIN = 15                 # retard maximal en minutes
UTC = timezone.utc


def iso(dt):
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def weighted_choice(rng, items, weights):
    return rng.choices(items, weights=weights, k=1)[0]


def new_device_id(rng, used):
    while True:
        d = "D-%06x" % rng.randrange(16 ** 6)
        if d not in used:
            used.add(d)
            return d


# --------------------------------------------------------------------------------------
# Referentiel merchants
# --------------------------------------------------------------------------------------
def generate_merchants(seed=42, n=300):
    rng = random.Random(seed * 7 + 1)
    cities = list(CITIES)
    cweights = [CITIES[c][0] for c in cities]
    cats = list(CATEGORIES)
    kweights = [CATEGORIES[k][0] for k in cats]
    merchants = []
    for i in range(1, n + 1):
        city = weighted_choice(rng, cities, cweights)
        cat = weighted_choice(rng, cats, kweights)
        _, lat, lon, _ = CITIES[city]
        name = "%s %s" % (rng.choice(CATEGORIES[cat][3]), rng.choice(NAME_SUFFIXES))
        merchants.append({
            "merchant_id": "M-%03d" % i,
            "name": name,
            "city": city,
            "category": cat,
            "latitude": round(lat + rng.uniform(-0.06, 0.06), 6),
            "longitude": round(lon + rng.uniform(-0.06, 0.06), 6),
        })
    return merchants


def load_or_create_merchants(path, seed):
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return [json.loads(line) for line in f if line.strip()]
    merchants = generate_merchants(seed)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for m in merchants:
            f.write(json.dumps(m, ensure_ascii=False) + "\n")
    print("[producer] referentiel cree : %s (%d lignes)" % (path, len(merchants)), file=sys.stderr)
    return merchants


# --------------------------------------------------------------------------------------
# Population de comptes, appareils et scenarios
# --------------------------------------------------------------------------------------
class World:
    """Population deterministe (wallets, appareils, cercles sociaux, fraude)."""

    def __init__(self, merchants, seed):
        rng = random.Random(seed)
        self.merchants = merchants
        self.merchants_by_city = {}
        for m in merchants:
            self.merchants_by_city.setdefault(m["city"], []).append(m)
        # popularite des commercants (loi de Zipf) : quelques commercants tres frequentes
        self.merchant_weight = {}
        for city, ms in self.merchants_by_city.items():
            order = list(range(len(ms)))
            rng.shuffle(order)
            for rank, idx in enumerate(order):
                self.merchant_weight[ms[idx]["merchant_id"]] = 1.0 / (rank + 1) ** 0.9

        cities = list(CITIES)
        cweights = [CITIES[c][0] for c in cities]
        used_devices = set()
        self.wallets = ["W-%d" % (WALLET_ID_START + i) for i in range(N_WALLETS)]
        self.home_city = {}
        self.device = {}
        # foyers de 1 a 4 comptes ; 12 % des foyers partagent un seul telephone (cas normal)
        i = 0
        while i < N_WALLETS:
            size = rng.choice([1, 1, 2, 2, 3, 4])
            members = self.wallets[i:i + size]
            city = weighted_choice(rng, cities, cweights)
            shared = len(members) > 1 and rng.random() < 0.12
            dev = new_device_id(rng, used_devices)
            for w in members:
                self.home_city[w] = city
                self.device[w] = dev if shared else new_device_id(rng, used_devices)
            i += size
        # cercles sociaux pour les virements P2P (famille / amis), 4 a 8 comptes
        shuffled = self.wallets[:]
        rng.shuffle(shuffled)
        self.circle = {}
        self.circles = []
        j = 0
        while j < len(shuffled):
            size = rng.randint(4, 8)
            c = shuffled[j:j + size]
            for w in c:
                self.circle[w] = len(self.circles)
            self.circles.append(c)
            j += size

        # ---- cas legitimes mais "atypiques" (sources de faux positifs) -------------
        # 2 telephones de teleboutique utilises par 5-6 clients chacun
        self.kiosk_devices = []
        pool = rng.sample(self.wallets, 12)
        for k in range(2):
            dev = new_device_id(rng, used_devices)
            self.kiosk_devices.append({"device_id": dev, "wallets": pool[k * 6:(k + 1) * 6][: 5 + k]})
        # 5 comptes "proprietaire / tontine" recevant des virements importants de 15-25 comptes
        self.landlords = []
        for w in rng.sample(self.wallets, 5):
            senders = rng.sample([x for x in self.wallets if x != w], rng.randint(15, 25))
            self.landlords.append({"wallet": w, "senders": senders})

        # ---- scenarios de fraude (verite terrain) --------------------------------
        frng = random.Random(seed * 1000 + 17)
        excluded = set()
        for k in self.kiosk_devices:
            excluded.update(k["wallets"])
        for l in self.landlords:
            excluded.add(l["wallet"])
        candidates = [w for w in self.wallets if w not in excluded]
        frng.shuffle(candidates)
        pos = 0

        def take(n):
            nonlocal pos
            out = candidates[pos:pos + n]
            pos += n
            return out

        self.shared_devices = []
        for _ in range(3):
            dev = new_device_id(frng, used_devices)
            self.shared_devices.append({"device_id": dev, "wallets": take(frng.randint(12, 18))})
        self.cycles = []
        for _ in range(4):
            self.cycles.append({"wallets": take(3), "base_amount": round(frng.uniform(3000, 9000), 2)})
        self.collectors = []
        for _ in range(3):
            col = take(1)[0]
            self.collectors.append({"wallet": col, "senders": take(frng.randint(35, 50))})

    def ground_truth(self):
        return {
            "description": "Scenarios de fraude injectes par producer.py (dependent uniquement de --seed)",
            "shared_devices": self.shared_devices,
            "circular_transfers": [c["wallets"] for c in self.cycles],
            "collector_accounts": [{"wallet": c["wallet"], "n_senders": len(c["senders"])} for c in self.collectors],
            "legit_atypical_for_false_positive_analysis": {
                "kiosk_devices": self.kiosk_devices,
                "landlord_accounts": [{"wallet": l["wallet"], "n_senders": len(l["senders"])} for l in self.landlords],
            },
        }

    # -------------------------------------------------------------------------------
    def pick_merchant(self, rng, city, categories=None):
        ms = self.merchants_by_city.get(city) or self.merchants
        if categories:
            ms2 = [m for m in ms if m["category"] in categories] or [m for m in self.merchants if m["category"] in categories]
            ms = ms2 or ms
        # 10 % des paiements se font hors de la ville de residence (deplacement)
        if not categories and rng.random() < 0.10:
            ms = self.merchants
        weights = [self.merchant_weight[m["merchant_id"]] for m in ms]
        return rng.choices(ms, weights=weights, k=1)[0]

    def amount_for(self, rng, category):
        _, median, sigma, _ = CATEGORIES[category]
        return round(max(2.0, min(20000.0, rng.lognormvariate(math.log(median), sigma))), 2)

    def normal_tx(self, rng):
        payer = rng.choice(self.wallets)
        city = self.home_city[payer]
        device = self.device[payer]
        r = rng.random()
        # utilisation d'un telephone de teleboutique (legitime)
        for k in self.kiosk_devices:
            if payer in k["wallets"] and rng.random() < 0.5:
                device = k["device_id"]
        if r < 0.012:  # virement vers un proprietaire / tontine (legitime, montants eleves)
            l = rng.choice(self.landlords)
            payer = rng.choice(l["senders"])
            return dict(type="P2P", payer_wallet=payer, payee_id=l["wallet"], device_id=self.device[payer],
                        city=self.home_city[payer], channel=rng.choice(["APP", "APP", "USSD"]),
                        amount_mad=round(rng.uniform(800, 3500), 2))
        if r < P2P_SHARE:
            if rng.random() < 0.95:
                circle = self.circles[self.circle[payer]]
                payee = rng.choice([w for w in circle if w != payer] or self.wallets)
            else:
                payee = rng.choice(self.wallets)
                if payee == payer:
                    payee = self.wallets[(self.wallets.index(payer) + 1) % N_WALLETS]
            amount = round(max(10.0, rng.lognormvariate(math.log(250), 0.9)), 2)
            return dict(type="P2P", payer_wallet=payer, payee_id=payee, device_id=device, city=city,
                        channel=rng.choices(["APP", "USSD"], weights=[75, 25])[0], amount_mad=amount)
        m = self.pick_merchant(rng, city)
        return dict(type="P2M", payer_wallet=payer, payee_id=m["merchant_id"], device_id=device, city=m["city"],
                    channel=rng.choices(["QR", "APP", "USSD"], weights=[70, 25, 5])[0],
                    amount_mad=self.amount_for(rng, m["category"]))

    def fraud_txs(self, rng):
        """Liste de toutes les transactions frauduleuses a injecter (ordre melange)."""
        out = []
        for sd in self.shared_devices:
            for w in sd["wallets"]:
                for _ in range(rng.randint(2, 4)):
                    m = self.pick_merchant(rng, self.home_city[w], categories=["e-commerce", "telecom"])
                    out.append(dict(type="P2M", payer_wallet=w, payee_id=m["merchant_id"], device_id=sd["device_id"],
                                    city=m["city"], channel="APP", amount_mad=round(rng.uniform(200, 1500), 2),
                                    _scenario="shared_device"))
        for cyc in self.cycles:
            a, b, c = cyc["wallets"]
            for rnd in range(3):
                amount = cyc["base_amount"] * (1 - 0.01 * rnd)
                for src, dst in ((a, b), (b, c), (c, a)):
                    amount = round(amount * rng.uniform(0.97, 0.995), 2)
                    out.append(dict(type="P2P", payer_wallet=src, payee_id=dst, device_id=self.device[src],
                                    city=self.home_city[src], channel="APP", amount_mad=amount,
                                    _scenario="circular_transfer"))
        for col in self.collectors:
            for s in col["senders"]:
                for _ in range(rng.choice([1, 1, 2])):
                    out.append(dict(type="P2P", payer_wallet=s, payee_id=col["wallet"], device_id=self.device[s],
                                    city=self.home_city[s], channel=rng.choice(["APP", "USSD"]),
                                    amount_mad=round(rng.uniform(10, 49), 2), _scenario="collector"))
        rng.shuffle(out)
        return out


# --------------------------------------------------------------------------------------
# Cycle de vie d'une transaction -> evenements
# --------------------------------------------------------------------------------------
def lifecycle(rng, tx_id, tx, t0, force_complete=False):
    base = {k: v for k, v in tx.items() if not k.startswith("_")}
    reject_mult = CITIES[base["city"]][3]
    events = []

    def ev(status, t):
        e = {"tx_id": tx_id, "type": base["type"], "payer_wallet": base["payer_wallet"], "payee_id": base["payee_id"],
             "device_id": base["device_id"], "city": base["city"], "channel": base["channel"], "status": status,
             "amount_mad": base["amount_mad"], "event_time": iso(t)}
        events.append((t, e))

    t = t0
    ev("INITIATED", t)
    if not force_complete and rng.random() < 0.035 * reject_mult:          # echec d'autorisation
        ev("REJECTED", t + timedelta(seconds=rng.randint(1, 5)))
        return events
    t = t + timedelta(seconds=rng.randint(1, 3))
    ev("AUTHORIZED", t)
    if not force_complete and rng.random() < 0.025 * reject_mult:          # rejet apres autorisation
        ev("REJECTED", t + timedelta(seconds=rng.randint(2, 6)))
        return events
    t = t + timedelta(seconds=rng.randint(1, 4))
    ev("COMPLETED", t)
    if not force_complete and rng.random() < 0.01:                         # annulation / remboursement
        ev("REVERSED", t + timedelta(seconds=rng.randint(60, 3600)))
    return events


def arrival_delay(rng):
    """Retourne (retard en secondes, est_en_retard)."""
    if rng.random() < LATE_SHARE:
        return rng.uniform(60, MAX_LATE_MIN * 60), True
    return rng.uniform(0, 2), False


# --------------------------------------------------------------------------------------
# Mode fichier
# --------------------------------------------------------------------------------------
def run_file(args, world):
    rng = random.Random(args.seed + 1)
    start = datetime.strptime(args.start, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    span = timedelta(days=args.days).total_seconds()
    est_tx = max(1, args.n // 3)
    frauds = world.fraud_txs(random.Random(args.seed + 2))
    fraud_slots = {}
    for idx, pos in enumerate(sorted(rng.sample(range(int(est_tx * 0.9)), len(frauds)))):
        fraud_slots.setdefault(pos, []).append(frauds[idx])

    rows = []  # (arrival_time, seq, event)
    n_events = n_late = n_dup = 0
    seq = 0
    tx_counter = args.start_id
    i = 0
    while n_events < args.n:
        t0 = start + timedelta(seconds=(i / est_tx) * span + rng.uniform(0, span / est_tx))
        batch = [(tx, False) for tx in fraud_slots.get(i, [])] + [(world.normal_tx(rng), True)]
        for tx, is_normal in batch:
            tx_id = "TX-%08d" % tx_counter
            tx_counter += 1
            for et, e in lifecycle(rng, tx_id, tx, t0, force_complete=not is_normal):
                d, late = arrival_delay(rng)
                n_late += late
                rows.append((et + timedelta(seconds=d), seq, e)); seq += 1
                n_events += 1
                if rng.random() < DUP_SHARE:
                    rows.append((et + timedelta(seconds=d + rng.uniform(0.5, 60)), seq, e)); seq += 1
                    n_dup += 1
                    n_events += 1
        i += 1
    rows.sort(key=lambda r: (r[0], r[1]))  # ordre d'arrivee : les evenements en retard arrivent plus tard
    rows = rows[: args.n]
    out = args.out or os.path.join(DEFAULT_DATA_DIR, "transactions_history.jsonl")
    os.makedirs(os.path.dirname(os.path.abspath(out)), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        for _, _, e in rows:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print("[producer] %d evenements ecrits dans %s (retards~%d, doublons~%d, tx frauduleuses=%d)"
          % (len(rows), out, n_late, n_dup, len(frauds)), file=sys.stderr)


# --------------------------------------------------------------------------------------
# Mode Kafka (temps reel)
# --------------------------------------------------------------------------------------
def run_kafka(args, world):
    from kafka import KafkaProducer  # pip install kafka-python

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all", linger_ms=20, retries=5,
    )
    rng = random.Random(args.seed + 11 + int(time.time()) if args.random_run else args.seed + 11)
    frauds = world.fraud_txs(random.Random(args.seed + 2))
    fraud_prob = 0.04  # ~4 % des nouvelles transactions tant qu'il reste des scenarios a injecter
    tx_rate = args.rate / 3.1  # ~3.1 evenements par transaction
    heap = []  # (send_at_epoch, seq, event, tag)
    seq = 0
    tx_counter = args.start_id
    sent = late = dup = 0
    t_start = time.time()
    next_tx = t_start
    next_stats = t_start + 10
    next_late_demo = t_start + 20 if args.late_demo_every else None
    log = open(args.log, "a", encoding="utf-8") if args.log else None

    def logline(msg):
        line = "%s %s" % (iso(datetime.now(UTC)), msg)
        print(line, flush=True)
        if log:
            log.write(line + "\n"); log.flush()

    logline("[producer] demarrage : topic=%s rate=%s ev/s bootstrap=%s" % (args.topic, args.rate, args.bootstrap))
    try:
        while True:
            now = time.time()
            if args.duration and now - t_start > args.duration:
                break
            if args.n and sent >= args.n:
                break
            # creation de nouvelles transactions selon un processus de Poisson
            while next_tx <= now:
                if frauds and rng.random() < fraud_prob:
                    tx, normal = frauds.pop(), False
                else:
                    tx, normal = world.normal_tx(rng), True
                tx_id = "TX-%08d" % tx_counter
                tx_counter += 1
                t0 = datetime.fromtimestamp(next_tx, UTC)
                for et, e in lifecycle(rng, tx_id, tx, t0, force_complete=not normal):
                    d, is_late = arrival_delay(rng)
                    send_at = et.timestamp() + d
                    heapq.heappush(heap, (send_at, seq, e, "LATE %.0fs" % d if is_late else "")); seq += 1
                    if rng.random() < DUP_SHARE:
                        heapq.heappush(heap, (send_at + rng.uniform(0.5, 30), seq, e, "DUP")); seq += 1
                next_tx += rng.expovariate(tx_rate)
            # demonstration explicite du watermark : un evenement 15 min en retard, un autre 5 min
            if next_late_demo and now >= next_late_demo:
                for minutes in (15, 5):
                    tx = world.normal_tx(rng)
                    tx_id = "TX-LATE%02dM-%06d" % (minutes, tx_counter); tx_counter += 1
                    et = datetime.now(UTC) - timedelta(minutes=minutes)
                    e = lifecycle(rng, tx_id, tx, et, force_complete=True)[-1][1]
                    producer.send(args.topic, key=e["tx_id"], value=e)
                    sent += 1
                    logline("[late-demo] envoi %s city=%s status=%s event_time=%s (retard %d min)"
                            % (e["tx_id"], e["city"], e["status"], e["event_time"], minutes))
                next_late_demo = now + args.late_demo_every
            # envoi des evenements arrives a echeance
            while heap and heap[0][0] <= now:
                _, _, e, tag = heapq.heappop(heap)
                producer.send(args.topic, key=e["tx_id"], value=e)
                sent += 1
                if tag.startswith("LATE"):
                    late += 1
                    if args.verbose:
                        logline("[late] %s %s event_time=%s %s" % (e["tx_id"], e["status"], e["event_time"], tag))
                elif tag == "DUP":
                    dup += 1
            if now >= next_stats:
                el = now - t_start
                logline("[producer] envoyes=%d (%.1f ev/s) retards=%d doublons=%d fraude_restante=%d en_attente=%d"
                        % (sent, sent / el, late, dup, len(frauds), len(heap)))
                next_stats = now + 10
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()
        logline("[producer] arret : %d evenements envoyes" % sent)
        if log:
            log.close()


# --------------------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Generateur de transactions SahlPay.ma")
    p.add_argument("--mode", choices=["merchants", "file", "kafka"], required=True)
    p.add_argument("--n", type=int, default=0, help="nombre d'evenements (file: defaut 50000 ; kafka: 0 = illimite)")
    p.add_argument("--rate", type=float, default=20.0, help="evenements par seconde (mode kafka)")
    p.add_argument("--duration", type=float, default=0, help="duree maximale en secondes (mode kafka)")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--out", default=None, help="fichier de sortie (mode file / merchants)")
    p.add_argument("--merchants", default=os.path.join(DEFAULT_DATA_DIR, "merchants.json"))
    p.add_argument("--truth", default=os.path.join(DEFAULT_DATA_DIR, "fraud_ground_truth.json"))
    p.add_argument("--start", default="2026-11-08T00:00:00Z", help="debut de l'historique (mode file)")
    p.add_argument("--days", type=float, default=7, help="duree couverte par l'historique (mode file)")
    p.add_argument("--start-id", type=int, default=None,
                   help="premier numero de tx (defaut: 1 en mode file, 10000000 en mode kafka)")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default="transactions")
    p.add_argument("--late-demo-every", type=float, default=0,
                   help="toutes les N secondes, envoie un evenement 15 min en retard et un 5 min en retard")
    p.add_argument("--random-run", action="store_true", help="flux normal different a chaque lancement")
    p.add_argument("--log", default=None, help="fichier de log (mode kafka)")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()

    if args.mode == "merchants":
        out = args.out or args.merchants
        if os.path.exists(out):
            os.remove(out)
        load_or_create_merchants(out, args.seed)
        return

    merchants = load_or_create_merchants(args.merchants, args.seed)
    world = World(merchants, args.seed)
    os.makedirs(os.path.dirname(os.path.abspath(args.truth)), exist_ok=True)
    with open(args.truth, "w", encoding="utf-8") as f:
        json.dump(world.ground_truth(), f, ensure_ascii=False, indent=2)

    if args.mode == "file":
        args.n = args.n or 50000
        args.start_id = args.start_id if args.start_id is not None else 1
        run_file(args, world)
    else:
        args.start_id = args.start_id if args.start_id is not None else 10_000_000
        run_kafka(args, world)


if __name__ == "__main__":
    main()
