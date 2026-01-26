import asyncio
import json
import math
import unicodedata
from pathlib import Path

from config import Config
from fetchers import Fetcher

class OSM:
    def get_rural_gminas_data(self, out_dir, voivodeship_name=Config.WARMINSKO_MAZURSKIE_TERC, **kwargs):
        out_dir = Path(out_dir)
        asyncio.run(
            self.download_rural_gminas_data(
                out_dir=out_dir,
                voivodeship_name=voivodeship_name,
                buffer_km=0.0,
            )
        )

    @staticmethod
    def is_rural_gmina(tags: dict) -> bool:
        terc = (tags.get("teryt:terc") or tags.get("terc") or "").strip()
        if terc and len(terc) >= 1:
            return terc[-1] in Config.RURAL_GMINA_TERC_SUFFIXES

        name = (tags.get("name") or "").lower()
        if "miasto" in name or name.startswith("m. "):
            return False
        place = (tags.get("place") or "").lower()
        if place in {"city", "town"}:
            return False
        return True

    @staticmethod
    def _slugify(value: str) -> str:
        norm = unicodedata.normalize("NFKD", value)
        ascii_text = "".join(c for c in norm if not unicodedata.combining(c))
        safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "-" for ch in ascii_text.lower())
        while "--" in safe:
            safe = safe.replace("--", "-")
        return safe.strip("-") or "gmina"

    @staticmethod
    def _expand_bbox(bbox: list[float], buffer_km: float) -> list[float]:
        if not buffer_km:
            return bbox
        minlon, minlat, maxlon, maxlat = bbox
        lat0 = (minlat + maxlat) / 2.0
        buffer_lat = buffer_km / 111.0
        cos_lat = math.cos(math.radians(lat0)) or 1e-6
        buffer_lon = buffer_km / (111.0 * cos_lat)
        return [
            minlon - buffer_lon,
            minlat - buffer_lat,
            maxlon + buffer_lon,
            maxlat + buffer_lat,
        ]

    async def download_rural_gminas_data(
        self,
        out_dir: Path,
        voivodeship_name: str,
        buffer_km: float,
    ) -> None:

        out_dir.mkdir(parents=True, exist_ok=True)

        async with Fetcher() as f:
            rel_id = await f.fetch_voivodeship_relation_id(voivodeship_name)
            gminas = await f.fetch_gminas_in_voivodeship(rel_id)

            rural_gminas = [g for g in gminas if self.is_rural_gmina(g.get("tags", {}))]


            index_path = out_dir / "rural_gminas_index.json"
            with index_path.open("w", encoding="utf-8") as f_out:
                json.dump(rural_gminas, f_out, ensure_ascii=False, indent=2)

            for g in rural_gminas:
                name = g.get("name") or f"gmina_{g.get('id')}"
                g_dir = out_dir / self._slugify(name)
                g_dir.mkdir(parents=True, exist_ok=True)

                bbox = g.get("bbox")
                if not bbox:
                    continue

                bbox = self._expand_bbox(bbox, buffer_km)

                stops_path = g_dir / "osm_stops.geojson"
                road_path = g_dir / "osm_road_network.geojson"
                buildings_path = g_dir / "osm_buildings.geojson"
                boundary_path = g_dir / "osm_boundary.geojson"

                await f.download_osm_transit_stops(bbox, str(stops_path))
                await f.download_osm_road_network(bbox, str(road_path))
                await f.download_osm_buildings(bbox, str(buildings_path))
                await f.download_osm_relation_boundary(g.get("id"), str(boundary_path))

