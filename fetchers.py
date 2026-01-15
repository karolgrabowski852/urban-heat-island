from pathlib import Path
from typing import Sequence
from urllib.parse import urlencode
import csv
import json
import math
import httpx
from config import Config
from utils import fallback
CHUNK_SIZE = 1024 * 1024


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


async def _stream_to_file_async(response: httpx.Response, out_path: Path) -> None:
    _ensure_parent(out_path)
    with out_path.open("wb") as f:
        async for chunk in response.aiter_bytes(chunk_size=CHUNK_SIZE):
            if chunk:
                f.write(chunk)


def _overpass_to_geojson(src: Path, dst: Path) -> None:
    with src.open("r", encoding="utf-8") as f:
        data = json.load(f)

    features: list[dict] = []
    for el in data.get("elements", []):
        tags = el.get("tags", {})

        if el.get("type") == "node" and "lat" in el and "lon" in el:
            geom = {
                "type": "Point",
                "coordinates": [el["lon"], el["lat"]],
            }
            features.append({"type": "Feature", "geometry": geom, "properties": tags})

        elif el.get("type") == "way" and "geometry" in el:
            coords = [[pt["lon"], pt["lat"]] for pt in el["geometry"]]
            geom = {
                "type": "LineString",
                "coordinates": coords,
            }
            features.append({"type": "Feature", "geometry": geom, "properties": tags})

    dst.parent.mkdir(parents=True, exist_ok=True)
    with dst.open("w", encoding="utf-8") as f:
        geojson_out = {
            "type": "FeatureCollection", 
            "crs": { "type": "name", "properties": { "name": "EPSG:4326" } },
            "features": features
        }
        json.dump(geojson_out, f)

class Fetcher:
    def __init__(self, timeout: int = Config.DEFAULT_TIMEOUT):
        self.client = httpx.AsyncClient(
            timeout=timeout,
        )

    @staticmethod
    def _bbox_to_overpass(bbox: Sequence[float]) -> str:
        """Zwraca bbox w kolejności south,west,north,east dla Overpass."""

        minlon, minlat, maxlon, maxlat = bbox
        return f"{minlat},{minlon},{maxlat},{maxlon}"
    
    @fallback()
    async def fetch_city_bbox(
        self,
        city: str,
        country: str | None = None,
        buffer_km: float = 0.0,
    ) -> list[float]:
        query = city if not country else f"{city}, {country}"
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
            "addressdetails": 0,
            "polygon_geojson": 0,
        }
        headers = {"User-Agent": Config.NOMINATIM_USER_AGENT}

        resp = await self.client.get(Config.NOMINATIM_SEARCH_URL, params=params, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            raise ValueError(f"Nie znaleziono miasta: {query}")

        bbox_raw = data[0].get("boundingbox")
        if not bbox_raw or len(bbox_raw) != 4:
            raise ValueError("Brak bounding box w odpowiedzi Nominatim")

        south, north, west, east = map(float, bbox_raw)
        lat0 = (south + north) / 2.0
        buffer_lat = buffer_km / 111.0
        cos_lat = math.cos(math.radians(lat0)) or 1e-6
        buffer_lon = buffer_km / (111.0 * cos_lat)

        return [
            west - buffer_lon,
            south - buffer_lat,
            east + buffer_lon,
            north + buffer_lat,
        ]

    async def close(self) -> None:
        await self.client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    @fallback()
    async def download_overpass(self, query: str, out_path: str) -> Path:
        payload = urlencode({"data": query}, doseq=False, encoding="utf-8")
        resp = await self.client.post(
            Config.OVERPASS_API_URL,
            content=payload.encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            },
        )
        resp.raise_for_status()
        out = Path(out_path)
        await _stream_to_file_async(resp, out)
        return out
    
    @fallback()
    async def download_osm_transit_stops(
        self,
        bbox: Sequence[float],
        out_path: str,
    ) -> Path:
        swne = self._bbox_to_overpass(bbox)
        query = f"""
[out:json][timeout:120];
(
  node["public_transport"="platform"]({swne});
  node["public_transport"="stop_position"]({swne});
  node["highway"="bus_stop"]({swne});
  node["railway"="tram_stop"]({swne});
);
out geom;
"""
        tmp = Path(out_path).with_suffix(".osm.json")
        await self.download_overpass(query, str(tmp))
        _overpass_to_geojson(tmp, Path(out_path))
        return Path(out_path)
    
    @fallback()
    async def download_osm_walk_network(
        self,
        bbox: Sequence[float],
        out_path: str,
    ) -> Path:
        swne = self._bbox_to_overpass(bbox)
        # Force only ways, exclude areas/polygons which might be recognized as areas by OGR/ArcGIS
        # Sometimes footways area defined as areas (parks etc), let's stick to lines.
        # This query is fine, but _overpass_to_geojson might be mixing types if some ways are closed.
        query = f"""
[out:json][timeout:120];
(
  way["highway"~"^(footway|path|pedestrian|living_street|residential|service|tertiary|secondary|primary)$"]({swne})["foot"!="no"]["area"!="yes"];
  way["highway"="cycleway"]({swne})["foot"!="no"]["area"!="yes"];
  way["sidewalk"]({swne});
);
out geom;
"""
        tmp = Path(out_path).with_suffix(".osm.json")
        await self.download_overpass(query, str(tmp))
        _overpass_to_geojson(tmp, Path(out_path))
        return Path(out_path)
    
    @fallback()
    async def download_osm_buildings(
        self,
        bbox: Sequence[float],
        out_path: str,
    ) -> Path:
        """Pobiera budynki z OSM (warstwa poligonowa)."""
        swne = self._bbox_to_overpass(bbox)
        query = f"""
[out:json][timeout:120];
(
  way["building"]({swne});
  relation["building"]({swne});
);
out geom;
"""
        tmp = Path(out_path).with_suffix(".osm.json")
        await self.download_overpass(query, str(tmp))
        _overpass_to_geojson(tmp, Path(out_path))
        return Path(out_path)

    @fallback()
    async def download_bdl_population(
        self,
        unit_id: str,
        year: int,
        out_path: str,
        var_id: str = Config.BDL_POPULATION_VAR,
    ) -> Path:
        async def _fetch(uid: str, y: int) -> httpx.Response:
            params = {
                "var-id": var_id,
                "year": str(y),
                "format": "json",
            }
            url = f"{Config.BDL_API_BASE}/data/by-unit/{uid.strip()}"
            headers = {
                "Accept": "application/json",
                "X-ClientId": Config.BDL_CLIENT_ID,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
            }
            return await self.client.get(url, params=params, headers=headers)

        years_to_try = [year, year - 1, year - 2]
        resp: httpx.Response | None = None
        for y_try in years_to_try:
            candidate = await _fetch(unit_id, y_try)
            resp = candidate
            if candidate.status_code < 400:
                year = y_try
                break

        if resp is None:
            raise RuntimeError("No response from BDL")

        resp.raise_for_status()
        data = resp.json()
        results = data.get("results", [])

        out = Path(out_path)
        _ensure_parent(out)
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["unit_id", "var_id", "year", "val", "attr"])
            for row in results:
                writer.writerow([
                    row.get("id"),
                    row.get("variable"),
                    row.get("year"),
                    row.get("val"),
                    row.get("attr_id"),
                ])

        return out