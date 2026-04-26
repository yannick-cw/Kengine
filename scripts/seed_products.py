#!/usr/bin/env python3
"""Seed the products index with N synthetic but REWE-shaped products.

Each product has: title, category, organic, price.
Title format: "[Brand] [Adjective] [Noun] [Variant] [Size]" — same shape as
real REWE listings ("Andechser Natur Bio Joghurt Griechischer Art 400g").

Vocabulary, sizes and price ranges are per-category so a "Milch" doesn't end
up in "snacks" with a 30 EUR price tag.

Usage:
    ./seed_products.py [--count 10000] [--host http://localhost:3333]
                       [--workers 16] [--create-index]
"""

import argparse
import concurrent.futures as cf
import json
import random
import sys
import time
import urllib.error
import urllib.request


# Category vocab. Each entry is (slug, spec) where spec has:
#   brands  : preferred brand names
#   adjs    : flavor / quality words (often empty -> no adjective)
#   nouns   : product types
#   variants: optional descriptor inserts (e.g. "3,5%", "geräuchert"); "" common
#   sizes   : pack-size strings
#   price   : (min_cents, max_cents)
#   organic_chance: probability isOrganic=True
CATEGORIES: dict[str, dict] = {
    "dairy": {
        "brands": [
            "Weihenstephan", "Andechser", "Bauer", "Müller", "Landliebe",
            "Berchtesgadener", "Alnatura", "REWE Bio", "ja!", "Almighurt",
        ],
        "adjs": ["", "", "Natur", "Cremig", "Griechischer Art", "Bio"],
        "nouns": [
            "Vollmilch", "H-Milch", "Joghurt", "Quark", "Schmand",
            "Sahne", "Frischkäse", "Buttermilch", "Kefir", "Butter",
            "Mascarpone", "Skyr",
        ],
        "variants": ["1,5%", "3,5%", "0,1%", "fettarm", "laktosefrei", ""],
        "sizes": ["150g", "200g", "250g", "400g", "500g", "1l", "500ml", "1kg"],
        "price": (89, 499),
        "organic_chance": 0.35,
    },
    "bakery": {
        "brands": ["Harry", "Lieken", "REWE Beste Wahl", "Mestemacher", "Golden Toast"],
        "adjs": ["", "Vollkorn", "Mehrkorn", "Steinofen", "Roggen", "Dinkel"],
        "nouns": ["Brot", "Toast", "Brötchen", "Baguette", "Knäckebrot", "Brezel"],
        "variants": ["", "", "geschnitten", "rustikal", "kerniges", ""],
        "sizes": ["250g", "400g", "500g", "750g", "1kg"],
        "price": (89, 399),
        "organic_chance": 0.15,
    },
    "produce": {
        "brands": ["", "REWE", "REWE Bio", "Demeter"],
        "adjs": ["", "", "frisch", "Klasse I"],
        "nouns": [
            "Äpfel", "Bananen", "Orangen", "Zitronen", "Avocado",
            "Tomaten", "Salatgurke", "Karotten", "Kartoffeln", "Zwiebeln",
            "Brokkoli", "Paprika", "Spinat", "Champignons", "Auberginen",
        ],
        "variants": ["", "", "rot", "grün", "lose"],
        "sizes": ["500g", "1kg", "1,5kg", "2kg", "Bund", "Stück"],
        "price": (99, 599),
        "organic_chance": 0.25,
    },
    "meat": {
        "brands": ["Wiltmann", "Rügenwalder", "Herta", "REWE Beste Wahl", "Gutfried"],
        "adjs": ["", "", "fein", "luftgetrocknet", "geräuchert"],
        "nouns": [
            "Schinken", "Salami", "Mortadella", "Lyoner", "Bratwurst",
            "Hähnchenbrust", "Hackfleisch", "Rinderfilet", "Putenbrust",
            "Leberkäse",
        ],
        "variants": ["", "", "am Stück", "geschnitten", "geräuchert", "gepökelt"],
        "sizes": ["100g", "150g", "200g", "300g", "500g", "1kg"],
        "price": (149, 1899),
        "organic_chance": 0.10,
    },
    "fish": {
        "brands": ["Iglo", "Followfish", "Costa", "REWE Beste Wahl", "Gottfried Friedrichs"],
        "adjs": ["", "", "MSC", "Wildfang", "Atlantik"],
        "nouns": ["Lachs", "Thunfisch", "Hering", "Forelle", "Garnelen", "Kabeljau"],
        "variants": ["", "", "geräuchert", "in Olivenöl", "natur", "TK"],
        "sizes": ["100g", "150g", "200g", "250g", "500g"],
        "price": (199, 2499),
        "organic_chance": 0.10,
    },
    "frozen": {
        "brands": ["Iglo", "Dr. Oetker", "Wagner", "Bofrost", "REWE Beste Wahl"],
        "adjs": ["", "", "klassisch", "knusprig"],
        "nouns": [
            "Pizza", "Pommes", "Spinat", "Erbsen", "Fischstäbchen",
            "Lasagne", "Kroketten", "Beerenmischung", "Eiscreme", "Frühlingsrollen",
        ],
        "variants": ["", "", "TK", "Margherita", "Salami", "Vanille", "Schoko"],
        "sizes": ["300g", "400g", "500g", "750g", "1kg", "2,5kg"],
        "price": (149, 999),
        "organic_chance": 0.10,
    },
    "beverages": {
        "brands": [
            "Coca-Cola", "Pepsi", "Volvic", "Gerolsteiner", "Granini",
            "Hohes C", "Fritz-Kola", "Apollinaris", "REWE Bio", "True Fruits",
        ],
        "adjs": ["", "", "naturtrüb", "still", "medium", "spritzig"],
        "nouns": [
            "Mineralwasser", "Apfelsaft", "Orangensaft", "Multivitamin",
            "Cola", "Limonade", "Eistee", "Smoothie", "Tonic Water", "Tafelwasser",
        ],
        "variants": ["", "", "ohne Zucker", "Zero", "light"],
        "sizes": ["0,33l", "0,5l", "0,75l", "1l", "1,25l", "1,5l", "2l"],
        "price": (49, 499),
        "organic_chance": 0.15,
    },
    "snacks": {
        "brands": [
            "Lorenz", "Funny-frisch", "Chio", "Pringles", "Bahlsen",
            "Seeberger", "REWE Beste Wahl",
        ],
        "adjs": ["", "", "knusprig", "salzig", "würzig"],
        "nouns": [
            "Chips", "Salzstangen", "Erdnüsse", "Cracker", "Reiswaffeln",
            "Nussmischung", "Tortillas", "Studentenfutter",
        ],
        "variants": ["", "", "Paprika", "Sour Cream", "Salt & Vinegar", "geröstet"],
        "sizes": ["100g", "150g", "175g", "200g", "300g"],
        "price": (99, 399),
        "organic_chance": 0.10,
    },
    "cereals": {
        "brands": [
            "Kölln", "Kellogg's", "Nestlé", "Seitenbacher", "mymuesli",
            "Alnatura", "REWE Bio",
        ],
        "adjs": ["", "", "knuspriges", "klassisches", "Energie"],
        "nouns": [
            "Müsli", "Haferflocken", "Cornflakes", "Granola", "Porridge",
            "Knuspermüsli", "Schokomüsli",
        ],
        "variants": ["", "", "mit Beeren", "mit Nüssen", "Schoko", "Frucht"],
        "sizes": ["375g", "500g", "750g", "1kg"],
        "price": (149, 599),
        "organic_chance": 0.30,
    },
    "condiments": {
        "brands": ["Maggi", "Knorr", "Heinz", "Hela", "Thomy", "Bertolli", "REWE Bio"],
        "adjs": ["", "", "extra natives", "mild", "scharf"],
        "nouns": [
            "Olivenöl", "Sonnenblumenöl", "Apfelessig", "Balsamico",
            "Ketchup", "Mayonnaise", "Senf", "Sojasauce", "Kräuter",
        ],
        "variants": ["", "", "klassisch", "fein", "süß", ""],
        "sizes": ["250ml", "500ml", "750ml", "1l"],
        "price": (149, 1499),
        "organic_chance": 0.20,
    },
    "sweets": {
        "brands": [
            "Milka", "Lindt", "Ritter Sport", "Ferrero", "Storck",
            "Haribo", "Katjes", "Trolli", "Alpia",
        ],
        "adjs": ["", "", "zartschmelzend", "extra", "feinste"],
        "nouns": [
            "Schokolade", "Pralinen", "Bonbons", "Gummibärchen",
            "Lakritz", "Riegel", "Kekse", "Waffeln", "Marzipan",
        ],
        "variants": ["", "", "Vollmilch", "Zartbitter", "Weiß", "Nuss", "Karamell"],
        "sizes": ["100g", "200g", "300g", "500g"],
        "price": (79, 599),
        "organic_chance": 0.10,
    },
    "pantry": {
        "brands": [
            "Barilla", "Buitoni", "Uncle Ben's", "Rapunzel", "Davert",
            "REWE Beste Wahl", "ja!",
        ],
        "adjs": ["", "", "feiner", "italienische"],
        "nouns": [
            "Spaghetti", "Penne", "Fusilli", "Tagliatelle",
            "Basmatireis", "Jasminreis", "Risottoreis", "Linsen",
            "Mehl", "Zucker", "Backpulver", "Gries",
        ],
        "variants": ["", "", "vollkorn", "glutenfrei", ""],
        "sizes": ["250g", "500g", "1kg", "2kg"],
        "price": (89, 499),
        "organic_chance": 0.20,
    },
    "deli": {
        "brands": ["Tartex", "Bonduelle", "Kühne", "Hengstenberg", "Obela", "REWE Bio"],
        "adjs": ["", "", "mediterran", "italienisch", "griechisch"],
        "nouns": [
            "Oliven", "Hummus", "Pesto", "Antipasti", "Feta",
            "Tzatziki", "Salat", "Aufstrich",
        ],
        "variants": ["", "", "klassisch", "scharf", "Kräuter", ""],
        "sizes": ["120g", "150g", "200g", "270g"],
        "price": (149, 599),
        "organic_chance": 0.25,
    },
    "alcohol": {
        "brands": [
            "Krombacher", "Bitburger", "Beck's", "Warsteiner", "Erdinger",
            "Jägermeister", "Hennessy", "Mumm", "Rotkäppchen", "Casillero del Diablo",
        ],
        "adjs": ["", "", "Premium", "Pils", "Weizen"],
        "nouns": [
            "Pils", "Weizenbier", "Helles", "Lager", "Riesling",
            "Pinot Grigio", "Cabernet Sauvignon", "Sekt", "Gin", "Wodka",
        ],
        "variants": ["", "", "trocken", "halbtrocken", "alkoholfrei", ""],
        "sizes": ["0,33l", "0,5l", "0,7l", "0,75l", "1l"],
        "price": (149, 3999),
        "organic_chance": 0.05,
    },
    "household": {
        "brands": [
            "Persil", "Ariel", "Lenor", "Frosch", "Sagrotan",
            "Tempo", "Zewa", "Domestos", "Pril",
        ],
        "adjs": ["", "", "ultra", "sensitive", "extra"],
        "nouns": [
            "Waschmittel", "Weichspüler", "Spülmittel", "Allzweckreiniger",
            "Toilettenpapier", "Küchenrolle", "Taschentücher", "Müllbeutel",
        ],
        "variants": ["", "", "Color", "Universal", "Frisch", "Lavendel"],
        "sizes": ["8 Stück", "1l", "1,5l", "2l", "20WL", "40WL"],
        "price": (99, 1499),
        "organic_chance": 0.05,
    },
}


def _pick(rng: random.Random, xs: list[str]) -> str:
    return rng.choice(xs)


def make_product(rng: random.Random) -> dict:
    cat = rng.choice(list(CATEGORIES.keys()))
    spec = CATEGORIES[cat]
    parts = []
    brand = _pick(rng, spec["brands"])
    if brand:
        parts.append(brand)
    organic = rng.random() < spec["organic_chance"]
    # Organic markers in title for some products, mirrors REWE conventions.
    if organic and rng.random() < 0.6 and "Bio" not in (brand or ""):
        parts.append("Bio")
    adj = _pick(rng, spec["adjs"])
    if adj:
        parts.append(adj)
    parts.append(_pick(rng, spec["nouns"]))
    variant = _pick(rng, spec["variants"])
    if variant:
        parts.append(variant)
    parts.append(_pick(rng, spec["sizes"]))

    lo, hi = spec["price"]
    price_cents = rng.randint(lo, hi)
    # 70% of REWE prices end in 9 (e.g. 1,99).
    if rng.random() < 0.7:
        price_cents = (price_cents // 10) * 10 + 9

    return {
        "title": " ".join(parts),
        "category": cat,
        "organic": organic,
        "price": round(price_cents / 100.0, 2),
    }


def post_one(host: str, index: str, doc: dict, timeout: float) -> tuple[bool, str]:
    url = f"{host}/indexes/{index}/documents"
    data = json.dumps(doc).encode("utf-8")
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            resp.read()
            return True, ""
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read()[:200].decode('utf-8', 'replace')}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def create_index(host: str, index: str) -> None:
    url = f"{host}/indexes/{index}"
    body = json.dumps({
        "fields": [
            {"fieldName": "title", "sType": "Text"},
            {"fieldName": "category", "sType": "Keyword"},
            {"fieldName": "organic", "sType": "Bool"},
            {"fieldName": "price", "sType": "Number"},
        ]
    }).encode("utf-8")
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"}, method="PUT"
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp.read()
            print(f"Created index '{index}'")
    except urllib.error.HTTPError as e:
        print(f"Index create returned HTTP {e.code} (may already exist): "
              f"{e.read()[:200].decode('utf-8', 'replace')}")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--count", type=int, default=10_000)
    p.add_argument("--host", default="http://localhost:3333")
    p.add_argument("--index", default="products")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--timeout", type=float, default=30.0)
    p.add_argument("--create-index", action="store_true",
                   help="Create the index before seeding")
    p.add_argument("--preview", type=int, default=0,
                   help="Print N example products and exit (no HTTP)")
    args = p.parse_args()

    rng = random.Random(args.seed)

    if args.preview > 0:
        for _ in range(args.preview):
            print(json.dumps(make_product(rng), ensure_ascii=False))
        return 0

    if args.create_index:
        create_index(args.host, args.index)

    products = [make_product(rng) for _ in range(args.count)]

    start = time.monotonic()
    ok = 0
    errors: list[str] = []
    with cf.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [
            pool.submit(post_one, args.host, args.index, doc, args.timeout)
            for doc in products
        ]
        for i, fut in enumerate(cf.as_completed(futures), 1):
            success, err = fut.result()
            if success:
                ok += 1
            elif len(errors) < 10:
                errors.append(err)
            if i % 500 == 0 or i == args.count:
                elapsed = time.monotonic() - start
                rate = i / elapsed if elapsed > 0 else 0.0
                print(f"  {i}/{args.count}  ok={ok}  "
                      f"{rate:.0f} req/s  elapsed={elapsed:.1f}s")

    elapsed = time.monotonic() - start
    print(f"\nDone: {ok}/{args.count} succeeded in {elapsed:.1f}s "
          f"({ok / elapsed:.0f} req/s)")
    if errors:
        print(f"\nFirst {len(errors)} errors:")
        for e in errors:
            print(f"  - {e}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
