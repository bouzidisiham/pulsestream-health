import csv 
import json
from pathlib import Path


def write_csv(rows: list[dict], path: Path) -> None : 
    if rows == None : 
        raise ValueError("Pas de données")
    else : 
        with open(path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f,fieldnames = list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)
    

def write_json(rows: list[dict], path: Path) -> None : 
    if rows == None : 
        raise ValueError("Pas de données")
    else : 
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, indent=2, ensure_ascii=False)