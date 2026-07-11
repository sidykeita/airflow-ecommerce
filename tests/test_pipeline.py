import pandas as pd
import pytest

from scripts.pipeline_logic import analyze_category, compute_kpis, filter_valid_rows


def _sample_df():
    return pd.DataFrame({
        "ID_Commande": ["A1", "A1", "A2", None, "A4", "A5"],
        "Date": ["2026-01-01"] * 6,
        "Produit": ["MUG", "CANDLE HOLDER", "TOY CAR", "BAG", "PEN", "MUG"],
        "Quantite": [2, 3, 0, 1, -1, 5],
        "Prix": [5.0, 10.0, 8.0, 4.0, 2.0, 5.0],
        "Montant": [10.0, 30.0, 0.0, -4.0, -2.0, 25.0],
        "Region": ["France"] * 6,
        "Client": [1, 1, 2, 3, 4, 5],
        "Categorie": ["Cuisine", "Deco", "Jouets_Loisirs", "Textile", "Papeterie", "Cuisine"],
    })


class TestFilterValidRows:
    def test_rejette_montant_negatif(self):
        valid_df, invalid_df = filter_valid_rows(_sample_df())
        assert not (valid_df["Montant"] < 0).any()

    def test_rejette_quantite_nulle_ou_negative(self):
        valid_df, invalid_df = filter_valid_rows(_sample_df())
        assert (valid_df["Quantite"] > 0).all()

    def test_rejette_id_commande_manquant(self):
        valid_df, invalid_df = filter_valid_rows(_sample_df())
        assert valid_df["ID_Commande"].notna().all()

    def test_conserve_les_lignes_valides(self):
        valid_df, invalid_df = filter_valid_rows(_sample_df())
        assert len(valid_df) == 3
        assert len(invalid_df) == 3


class TestComputeKpis:
    def test_kpis_sur_donnees_valides(self):
        valid_df, _ = filter_valid_rows(_sample_df())
        kpis = compute_kpis(valid_df)
        assert kpis["nb_commandes"] == 2
        assert kpis["nb_clients"] == 2
        assert kpis["chiffre_affaires"] == 65.0
        assert kpis["panier_moyen"] == 32.5

    def test_panier_moyen_zero_si_aucune_commande(self):
        empty_df = _sample_df().iloc[0:0]
        kpis = compute_kpis(empty_df)
        assert kpis["panier_moyen"] == 0.0


class TestAnalyzeCategory:
    def test_revenue_par_categorie(self):
        valid_df, _ = filter_valid_rows(_sample_df())
        result = analyze_category(valid_df, "Cuisine")
        assert result["category"] == "Cuisine"
        assert result["revenue"] == 35.0


def test_dag_structure():
    """Verifie la structure du DAG. Necessite apache-airflow (Docker/Jenkins)."""
    pytest.importorskip("airflow")
    from dags.ecommerce_sales_pipeline import CATEGORIES, dag

    assert dag.dag_id == "ecommerce_sales_pipeline"

    task_ids = set(dag.task_ids)
    expected = {
        "wait_for_file", "check_file_exists", "check_file_not_empty",
        "check_data_quality", "decide_execution_path", "stop_pipeline",
        "load_data", "compute_global_kpis", "generate_report", "store_mongodb",
    }
    assert expected.issubset(task_ids)

    for category in CATEGORIES:
        assert f"analyze_category_{category}" in task_ids