"""Genere un echantillon SYNTHETIQUE au meme format que le dataset Kaggle REES46.
Sert uniquement a tester le pipeline avant d'avoir telecharge le vrai dataset.
Ne jamais presenter ces resultats comme des resultats reels."""
import csv, random, sys
from datetime import datetime, timedelta

random.seed(42)
n = int(sys.argv[1]) if len(sys.argv) > 1 else 50000
out = sys.argv[2] if len(sys.argv) > 2 else "data/sample/sample.csv"

cats = ["electronics.smartphone", "electronics.audio.headphone", "appliances.kitchen.washer",
        "computers.notebook", "apparel.shoes", "furniture.living_room.sofa", None]
brands = ["samsung", "apple", "xiaomi", "lg", "sony", "nike", None]
products = [(1000000 + i, random.choice(cats), random.choice(brands), round(random.uniform(5, 900), 2))
            for i in range(300)]
start = datetime(2019, 10, 1)

with open(out, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["event_time", "event_type", "product_id", "category_id", "category_code",
                "brand", "price", "user_id", "user_session"])
    for i in range(n):
        user = random.randint(1, 3000)
        session = f"sess-{user}-{random.randint(1, 3)}"
        t = start + timedelta(seconds=random.randint(0, 7 * 86400))
        pid, cat, brand, price = random.choice(products)
        r = random.random()
        ev = "view" if r < 0.85 else "cart" if r < 0.93 else "remove_from_cart" if r < 0.96 else "purchase"
        row = [t.strftime("%Y-%m-%d %H:%M:%S UTC"), ev, pid, 2053013555631882655,
               cat or "", brand or "", price, user, session]
        w.writerow(row)
        # quelques anomalies volontaires pour tester le nettoyage
        if i % 500 == 0:
            w.writerow(row)                                   # doublon
        if i % 700 == 0:
            w.writerow([row[0], "view", pid, 1, cat or "", brand or "", -1.0, user, session])  # prix negatif
        if i % 900 == 0:
            w.writerow([row[0], "unknown_event", pid, 1, "", "", price, user, session])        # type invalide
        if i % 1100 == 0:
            w.writerow([row[0], "view", pid, 1, "", "", price, "", session])                   # user_id manquant
print(f"Echantillon ecrit dans {out}")
