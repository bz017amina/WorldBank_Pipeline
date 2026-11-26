import requests

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = [
    "NY.GDP.MKTP.CD",
    "FP.CPI.TOTL.ZG",
    "SP.POP.TOTL",
    "SL.UEM.TOTL.ZS",
    "SP.DYN.LE00.IN",
]

for country in COUNTRIES:
    for ind in INDICATORS:
        url = f"https://api.worldbank.org/v2/country/{country}/indicator/{ind}?format=json&per_page=10"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        # Vérifier la présence d'un message d'erreur JSON malgré le statut 200
        if not isinstance(data, list) or len(data) < 2 or not isinstance(data[1], list):
            raise RuntimeError(f"Réponse inattendue pour {country} / {ind} : {data}")
        if isinstance(data[0], dict) and "message" in data[0]:
            raise RuntimeError(f"Erreur API pour {country} / {ind} : {data[0]['message']}")

        sample = next((row for row in data[1] if row.get("value") is not None), None)
        print(f"OK | {country} | {ind} | nb_lignes={len(data[1])} | exemple={sample['date'] if sample else 'None'}:{sample['value'] if sample else 'None'}")