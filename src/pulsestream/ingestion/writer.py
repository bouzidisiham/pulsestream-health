import csv 
import json
from pathlib import Path


def write_csv(rows: list[dict], path: Path) -> None : 
    """Orchestre la génération complète des données du projet.

    Args:
        n: Nombre de patients à générer.
        start: Date et heure de début des mesures.
        n_mesures: Nombre de mesures par patient.

    Returns:
        Un tuple (patients, vitals) contenant les deux listes de dicts.
    """
    if rows == None : 
        raise ValueError("Pas de données")
    else : 
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f,fieldnames = list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    

def write_json(rows: list[dict], path: Path) -> None : 
    """Écrit une liste de dictionnaires dans un fichier JSON indenté.

    Args:
        rows: Liste de dictionnaires à écrire.
        path: Chemin du fichier de sortie (objet Path).
    """
    if rows == None : 
        raise ValueError("Pas de données")
    else : 
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)