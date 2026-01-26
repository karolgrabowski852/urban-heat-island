import csv
import json
import logging
import unicodedata

from config import Config
from transit_accessibility_analysis import ArcGisPipeline

BASE_DIR = Config.BASE_DIR
DATA_DIR = BASE_DIR / "data" / "warmia-mazury-rural"
INDEX_PATH = DATA_DIR / "rural_gminas_index.json"
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def slugify(value: str) -> str:
    """Zwraca bezpieczny slug z nazwy gminy.

    Normalizuje nazwę do ASCII, zastępuje znaki niealfanumeryczne myślnikami
    i usuwa powtarzające się myślniki. Przydatne do tworzenia nazw katalogów.
    """
    norm = unicodedata.normalize("NFKD", value)
    ascii_text = "".join(c for c in norm if not unicodedata.combining(c))
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in ascii_text.lower())
    while "--" in safe:
        safe = safe.replace("--", "-")
    return safe.strip("-") or "gmina"


def process_gmina(model: ArcGisPipeline, gmina: dict) -> tuple[str, float]:
    """Wykonuje pełną analizę dla pojedynczej gminy i zwraca wynik.

    Funkcja przygotowuje ścieżki do plików wejściowych, sprawdza ich obecność,
    buduje sieć pieszą i uruchamia analizę obszarów obsługi. Zwraca nazwę gminy
    oraz procent budynków z dostępem do przystanku.
    """
    name = gmina.get("name") or f"gmina_{gmina.get('id')}"
    g_dir = DATA_DIR / slugify(name)

    stops = g_dir / "osm_stops.geojson"
    boundary = g_dir / "osm_boundary.geojson"
    road = g_dir / "osm_road_network.geojson"
    buildings = g_dir / "osm_buildings.geojson"

    for p in (stops, boundary, road, buildings):
        if not p.exists():
            raise FileNotFoundError(f"Brak pliku: {p}")

    gdb_path = g_dir / "service_area.gdb"
    model.workspace_gdb = str(gdb_path)

    network_dataset = model.build_network_dataset_from_roads(str(road))
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

    return name, access_pct


def main() -> None:
    """Uruchamia analizę dla wszystkich gmin z indeksu i zapisuje podsumowanie.

    Funkcja wczytuje listę gmin, iteruje po nich wywołując `process_gmina`, zapisuje
    wyniki do zbiorczego pliku CSV i loguje postęp dla każdej gminy. Tworzy model
    `ArcGisPipeline` z domyślnym geodatabase w katalogu wyjściowym.
    """
    if not INDEX_PATH.exists():
        raise FileNotFoundError(f"Brak indeksu gmin: {INDEX_PATH}")

    with INDEX_PATH.open("r", encoding="utf-8") as f:
        index = json.load(f)

    if not index:
        raise ValueError("Indeks gmin jest pusty.")

    output_csv = OUTPUT_DIR / "wyniki_wszystkie_gminy.csv"
    model = ArcGisPipeline(workspace_gdb=str(OUTPUT_DIR / "service_area.gdb"))

    with output_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["gmina", "procent_dostepu_do_przystanku"])

        for gmina in index:
            name, access_pct = process_gmina(model, gmina)
            writer.writerow([name, f"{access_pct:.2f}"])
            logger.info("Output: %s (%.2f%%)", name, access_pct)

    logger.info("Saved: %s", output_csv)


