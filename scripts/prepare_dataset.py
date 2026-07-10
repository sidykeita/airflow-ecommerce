import pandas as pd

RAW_PATH = "data/raw/online_retail.xlsx"
OUTPUT_PATH = "data/dataset.csv"

COLUMN_MAPPING = {
    "InvoiceNo": "ID_Commande",
    "InvoiceDate": "Date",
    "Description": "Produit",
    "Quantity": "Quantite",
    "UnitPrice": "Prix",
    "Country": "Region",
    "CustomerID": "Client",
}

FINAL_COLUMNS = [
    "ID_Commande", "Date", "Produit", "Quantite",
    "Prix", "Montant", "Region", "Client", "Categorie",
]

# Le dataset source n'a pas de colonne categorie : on la deduit du libelle
# produit par mots-cles. C'est une simplification assumee (documentee dans
# le rapport) : le vrai dataset n'a pas de dimension categorie exploitable.
# L'ORDRE compte : le premier mot-cle trouve dans le texte l'emporte.
CATEGORY_KEYWORDS = [
    ("Deco",               ["LIGHT", "CANDLE", "HOLDER", "LANTERN", "DECORATION", "ORNAMENT"]),
    ("Cuisine",            ["MUG", "CUP", "PLATE", "BOWL", "KITCHEN", "TEA", "COFFEE", "BOTTLE"]),
    ("Bijoux_Accessoires", ["NECKLACE", "BRACELET", "RING", "EARRING", "PURSE", "JEWEL"]),
    ("Papeterie",          ["CARD", "NOTEBOOK", "PEN ", "PAPER", "BOOK"]),
    ("Jouets_Loisirs",     ["TOY", "GAME", "PUZZLE", "BALL", "DOLL"]),
    ("Textile",            ["BAG", "SCARF", "HAT", "SOCK", "APRON", "SIGN"]),
]


def categorize(produit) -> str:
    text = str(produit).upper()
    for category, keywords in CATEGORY_KEYWORDS:
        if any(kw in text for kw in keywords):
            return category
    return "Autre"


def prepare_dataset(raw_path: str = RAW_PATH, output_path: str = OUTPUT_PATH) -> None:
    print(f"Lecture de {raw_path} ...")
    df = pd.read_excel(raw_path)

    df = df.rename(columns=COLUMN_MAPPING)

    df["Montant"] = df["Quantite"] * df["Prix"]
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
    df["Categorie"] = df["Produit"].apply(categorize)

    df = df[FINAL_COLUMNS]

    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"OK : {len(df)} lignes ecrites dans {output_path}")
    print(df["Categorie"].value_counts())
    print(df.head())


if __name__ == "__main__":
    prepare_dataset()