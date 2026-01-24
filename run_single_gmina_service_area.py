import json
import unicodedata
import csv
from pathlib import Path

from config import Config
from transit_accessibility_analysis import Workspace

BASE_DIR = Path(r"c:\geoinf\communication-exclusion")
DATA_DIR = BASE_DIR / "data" / "warmia-mazury-rural"
INDEX_PATH = DATA_DIR / "rural_gminas_index.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def slugify(value: str) -> str:
    norm = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(c for c in norm if not unicodedata.combining(c))
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in ascii_text.lower())
    while "--" in safe:
        safe = safe.replace("--", "-")
    return safe.strip("-") or "gmina"


def main() -> None:
    if not INDEX_PATH.exists():
        raise FileNotFoundError(f"Brak indeksu gmin: {INDEX_PATH}")

    with INDEX_PATH.open("r", encoding="utf-8") as f:
        index = json.load(f)

    if not index:
        raise ValueError("Indeks gmin jest pusty.")

    gmina = index[0]
    name = gmina.get("name") or f"gmina_{gmina.get('id')}"
    g_dir = DATA_DIR / slugify(name)

    stops = g_dir / "osm_stops.geojson"
    boundary = g_dir / "osm_boundary.geojson"
    walk = g_dir / "osm_walk_network.geojson"
    buildings = g_dir / "osm_buildings.geojson"

    for p in (stops, boundary, walk, buildings):
        if not p.exists():
            raise FileNotFoundError(f"Brak pliku: {p}")

    gdb_path = g_dir / "service_area.gdb"
    model = Workspace(workspace_gdb=str(gdb_path))

    if Config.NETWORK_DATASET_PATH:
        network_dataset = Config.NETWORK_DATASET_PATH
        travel_mode = "Walking Time"
        cutoffs = [5, 10, 15]
    else:
        network_dataset = model.build_network_dataset_from_walk(str(walk))
        meters_per_min = (model.walking_speed_kmh * 1000.0) / 60.0
        cutoffs = [c * meters_per_min for c in [5, 10, 15]]
        travel_mode = None

    service_area_polygons = model.run_service_area(
        network_dataset=network_dataset,
        stops_geojson=str(stops),
        boundary_geojson=str(boundary),
        travel_mode=travel_mode,
        cutoffs=cutoffs,
    )

    _, access_pct = model.calculate_building_access(
        buildings_geojson=str(buildings),
        service_area_polygons=service_area_polygons,
    )

    # Save results to CSV
    output_csv = OUTPUT_DIR / f"wynik_{slugify(name)}.csv"
    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["gmina", "procent_dostępu_do_przystanku"])
        writer.writerow([name, f"{access_pct:.2f}"])

    print(f"Wynik zapisany: {output_csv}")


if __name__ == "__main__":
    main()
