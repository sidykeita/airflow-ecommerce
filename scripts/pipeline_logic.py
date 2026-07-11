import pandas as pd


def filter_valid_rows(df: pd.DataFrame):
    """
    Applique les regles de gestion du sujet (section 8) :
      - ID_Commande manquant -> ligne invalide
      - Montant negatif -> ligne rejetee
      - Quantite nulle ou negative -> ligne invalide

    Retourne (valid_df, invalid_df).
    """
    invalid_mask = (
        df["ID_Commande"].isna()
        | (df["Montant"] < 0)
        | (df["Quantite"] <= 0)
    )
    valid_df = df[~invalid_mask].copy()
    invalid_df = df[invalid_mask].copy()
    return valid_df, invalid_df


def compute_kpis(df: pd.DataFrame) -> dict:
    """Indicateurs metier globaux (section 7 du sujet)."""
    nb_commandes = int(df["ID_Commande"].nunique())
    nb_clients = int(df["Client"].nunique())
    chiffre_affaires = float(df["Montant"].sum())
    panier_moyen = round(chiffre_affaires / nb_commandes, 2) if nb_commandes else 0.0
    return {
        "nb_commandes": nb_commandes,
        "nb_clients": nb_clients,
        "chiffre_affaires": round(chiffre_affaires, 2),
        "panier_moyen": panier_moyen,
    }


def analyze_category(df: pd.DataFrame, category: str) -> dict:
    subset = df[df["Categorie"] == category]
    return {
        "category": category,
        "orders": int(subset["ID_Commande"].nunique()),
        "revenue": round(float(subset["Montant"].sum()), 2),
    }