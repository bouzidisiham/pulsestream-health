import argparse 
from datetime import datetime
from pathlib import Path

from pulsestream.ingestion.generator import generate_all_data
from pulsestream.ingestion.writer import write_csv, write_json

def main() : 
    parser = argparse.ArgumentParser(
        description="Génère les données synthétiques du projet PulseStream"
    )
    parser.add_argument(
        "--n-patients", type=int, default=200,
        help="Nombre de patients à générer (défaut: 200)"
    )
    parser.add_argument(
        "--days", type=int, default=7,
        help="Nombre de jours de mesures par patient (défaut: 7)"
    )

    args = parser.parse_args()

    n_mesures = args.days * 12

    project_root = Path(__file__).parent.parent
    output_dir = project_root / "data" / "raw"

    maintenant = datetime.now()

    patients , vitals = generate_all_data(args.n_patients,maintenant,n_mesures)

    output_dir_patients_csv = output_dir / "patients.csv"
    write_csv(patients, output_dir_patients_csv)

    output_dir_patients_json =  output_dir / "patients.json"
    write_json(patients, output_dir_patients_json)

    output_dir_vitals_csv = output_dir / "vitals.csv"
    write_csv(vitals, output_dir_vitals_csv)

    output_dir_vitals_json = output_dir / "vitals.json"
    write_json(vitals, output_dir_vitals_json)
    
    total = args.n_patients * n_mesures

    print(f"{args.n_patients} patients générés")
    print(f"{n_mesures} mesures générées par patient")
    print(f"{total} mesures au total")
    print("Fichiers écrits : " )
    print("\n".join(f"- {f.name}" for f in sorted(output_dir.iterdir()) if f.suffix in {".csv",".json"}))


if __name__ == "__main__" : 
    main()