#!/usr/bin/env python3
"""Seed the products index with N synthetic products.

Usage:
    ./seed_products.py [--count 10000] [--host http://localhost:3333] [--workers 16] [--create-index]
"""

import argparse
import concurrent.futures as cf
import json
import random
import sys
import time
import urllib.error
import urllib.request

ADJECTIVES = [
    "Bio", "Frisch", "Klassisch", "Premium", "Vegan", "Glutenfrei",
    "Laktosefrei", "Regional", "Hausgemacht", "Knusprig", "Saftig",
    "Würzig", "Mild", "Scharf", "Süß", "Herzhaft", "Original", "Natur",
    "Vollkorn", "Fettarm", "Zuckerfrei", "Handgemacht", "Geräuchert",
]

NOUNS = [
    "Milch", "Joghurt", "Käse", "Butter", "Sahne", "Quark",
    "Brot", "Brötchen", "Haferflocken", "Müsli", "Reis", "Nudeln",
    "Apfel", "Banane", "Orange", "Tomate", "Gurke", "Karotte", "Kartoffel",
    "Zwiebel", "Salat", "Schinken", "Salami", "Wurst", "Hähnchen", "Lachs",
    "Thunfisch", "Eier", "Honig", "Marmelade", "Schokolade", "Keks",
    "Kuchen", "Kaffee", "Tee", "Saft", "Wasser", "Bier", "Wein",
    "Olivenöl", "Essig", "Senf", "Ketchup", "Mehl", "Zucker", "Salz",
]

VARIANTS = [
    "1.5%", "3.5%", "fettarm", "vollfett", "500g", "1kg", "200g", "100g",
    "klassisch", "extra", "light", "natur", "ungesüßt", "gesalzen",
    "ungesalzen", "geschnitten", "am Stück", "gefroren", "frisch",
]

CATEGORIES = [
    "dairy", "bakery", "produce", "meat", "fish", "frozen",
    "beverages", "snacks", "cereals", "condiments", "sweets",
    "pantry", "deli", "alcohol", "household",
]


def make_product(rng: random.Random) -> dict:
    parts = [rng.choice(ADJECTIVES), rng.choice(NOUNS)]
    if rng.random() < 0.6:
        parts.append(rng.choice(VARIANTS))
    return {
        "title": " ".join(parts),
        "category": rng.choice(CATEGORIES),
        "organic": rng.random() < 0.3,
        "price": round(rng.uniform(0.29, 49.99), 2),
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
    args = p.parse_args()

    if args.create_index:
        create_index(args.host, args.index)

    rng = random.Random(args.seed)
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
