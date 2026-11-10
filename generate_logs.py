import json, os, random
from datetime import timedelta
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

N = 1_000_000
OUT = "data/raw/logs.json"
os.makedirs("data/raw", exist_ok=True)

users = [f"U-{i}" for i in range(1000, 51000)]
products = [f"P-{i}" for i in range(100, 1100)]

count, sid = 0, 0
with open(OUT, "w") as f:
    while count < N:
        sid += 1
        user = random.choice(users)
        t = fake.date_time_between(start_date="-30d", end_date="now")
        in_cart = False
        for _ in range(random.randint(2, 15)):
            if count >= N:
                break
            t += timedelta(seconds=random.randint(1, 180))
            r = random.random()
            if r < 0.50:
                action = "page_view"
            elif r < 0.75:
                action = "click"
            elif r < 0.88:
                action = "add_to_cart"
                in_cart = True
            elif r < 0.93:
                action = "remove_from_cart"
            else:
                action = "purchase" if in_cart else "page_view"

            ev = {
                "user_id": user,
                "product_id": random.choice(products),
                "timestamp": t.strftime("%Y-%m-%d %H:%M:%S"),
                "action_type": action,
                "session_id": f"S-{sid}",
            }
            # ~1 % de valeurs manquantes (pour justifier le nettoyage)
            if random.random() < 0.01:
                ev[random.choice(["user_id", "product_id", "session_id"])] = None

            line = json.dumps(ev)
            f.write(line + "\n")
            count += 1
            # ~2 % de doublons exacts
            if random.random() < 0.02 and count < N:
                f.write(line + "\n")
                count += 1

print(f"{count} lignes écrites dans {OUT}")