import json
import unicodedata
from pathlib import Path

from transit_accessibility_analysis import Workspace

BASE_DIR = Path(r"c:\geoinf\communication-exclusion")
DATA_DIR = BASE_DIR / "data" / "warmia-mazury-rural"
INDEX_PATH = DATA_DIR / "rural_gminas_index.json"


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
    walk = g_dir / "osm_walk_network.geojson"
    buildings = g_dir / "osm_buildings.geojson"
    boundary = g_dir / "osm_boundary.geojson"

    for p in (stops, walk, buildings, boundary):
        if not p.exists():
            raise FileNotFoundError(f"Brak pliku: {p}")

    gdb_path = g_dir / "analysis.gdb"
    model = Workspace(workspace_gdb=str(gdb_path))

    model.run_analysis(
        stops_geojson=str(stops),
        walk_geojson=str(walk),
        buildings_wfs_json=str(buildings),
        boundary_geojson=str(boundary),
    )


if __name__ == "__main__":
    main()
