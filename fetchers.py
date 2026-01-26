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

        elif el.get("type") == "relation" and "members" in el:
            # Attempt to build polygons from relation members with geometry
            outer_rings: list[list[list[float]]] = []
            for member in el.get("members", []):
                if member.get("type") != "way" or "geometry" not in member:
                    continue
                coords = [[pt["lon"], pt["lat"]] for pt in member["geometry"]]
                if not coords:
                    continue
                if coords[0] != coords[-1]:
                    coords.append(coords[0])
                if member.get("role") == "outer":
                    outer_rings.append(coords)

            if outer_rings:
                if len(outer_rings) == 1:
                    geom = {"type": "Polygon", "coordinates": [outer_rings[0]]}
                else:
                    geom = {"type": "MultiPolygon", "coordinates": [[ring] for ring in outer_rings]}
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
    async def download_osm_road_network(
        self,
        bbox: Sequence[float],
        out_path: str,
    ) -> Path:
        """Download all road types from OSM: motorway, trunk, primary, secondary, tertiary, unclassified, residential, service, living_street, footway, path, pedestrian, cycleway, sidewalk."""
        swne = self._bbox_to_overpass(bbox)
        query = f"""
[out:json][timeout:120];
(
  way["highway"~"^(motorway|trunk|primary|secondary|tertiary|unclassified|residential|service|living_street|footway|path|pedestrian|cycleway)$"]({swne})["area"!="yes"];
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
    async def fetch_voivodeship_relation_id(self, identifier: str = "28") -> int:
        """
        Pobiera ID relacji województwa na podstawie kodu TERC (np. '28')
        lub nazwy (np. 'Warmińsko-Mazurskie').
        """
        ident = (identifier or "").strip()
        if ident.isdigit() and len(ident) <= 2:
            query = f"""
[out:json][timeout:60];
relation["boundary"="administrative"]["admin_level"="4"]["teryt:terc"="{ident}"];
out ids;
"""
            tmp = Path(f"data/tmp_voivodeship_{ident}.json")
            await self.download_overpass(query, str(tmp))
            with tmp.open("r", encoding="utf-8") as f:
                data = json.load(f)

            elements = data.get("elements", [])
            if elements:
                return int(elements[0]["id"])

            raise ValueError(f"Nie znaleziono województwa o kodzie TERC: {ident}")

        candidates = [ident]
        if ident.lower().startswith("województwo "):
            candidates.append(ident.replace("Województwo ", "", 1))
            candidates.append(ident.replace("województwo ", "", 1))
        if "warmińsko-mazurskie" in ident.lower() or "warminsko-mazurskie" in ident.lower():
            candidates.append("Warmińsko-Mazurskie")
            candidates.append("Warminsko-Mazurskie")

        for candidate in candidates:
            query = f"""
[out:json][timeout:120];
relation["boundary"="administrative"]["admin_level"="4"]["name"="{candidate}"];
out ids;
"""
            tmp = Path(f"data/tmp_{candidate.replace(' ', '_')}_voivodeship.json")
            await self.download_overpass(query, str(tmp))
            with tmp.open("r", encoding="utf-8") as f:
                data = json.load(f)
            elements = data.get("elements", [])
            if elements:
                return int(elements[0]["id"])

        name_pl = ident.replace("Województwo ", "").replace("województwo ", "")
        query = f"""
[out:json][timeout:120];
relation["boundary"="administrative"]["admin_level"="4"]["name:pl"~"{name_pl}"];
out ids;
"""
        tmp = Path(f"data/tmp_{ident.replace(' ', '_')}_voivodeship_pl.json")
        await self.download_overpass(query, str(tmp))
        with tmp.open("r", encoding="utf-8") as f:
            data = json.load(f)
        elements = data.get("elements", [])
        if not elements:
            raise ValueError(f"Nie znaleziono województwa: {identifier}")
        return int(elements[0]["id"])

    @fallback()
    async def fetch_gminas_in_voivodeship(self, voivodeship_rel_id: int) -> list[dict]:
        area_id = 3600000000 + voivodeship_rel_id
        query = f"""
[out:json][timeout:180];
area({area_id})->.searchArea;
relation["boundary"="administrative"]["admin_level"="7"](area.searchArea);
out tags center bb;
"""
        tmp = Path(f"data/tmp_gminas_{voivodeship_rel_id}.json")
        await self.download_overpass(query, str(tmp))
        with tmp.open("r", encoding="utf-8") as f:
            data = json.load(f)

        gminas: list[dict] = []
        for el in data.get("elements", []):
            if el.get("type") != "relation":
                continue
            tags = el.get("tags", {})
            bounds = el.get("bounds")
            bbox = None
            if bounds:
                bbox = [
                    float(bounds.get("minlon")),
                    float(bounds.get("minlat")),
                    float(bounds.get("maxlon")),
                    float(bounds.get("maxlat")),
                ]
            gminas.append({
                "id": int(el.get("id")),
                "name": tags.get("name", ""),
                "tags": tags,
                "bbox": bbox,
            })
        return gminas

    @fallback()
    async def download_osm_relation_boundary(
        self,
        relation_id: int,
        out_path: str,
    ) -> Path:
        query = f"""
[out:json][timeout:180];
relation({relation_id});
out geom;
"""
        tmp = Path(out_path).with_suffix(".osm.json")
        await self.download_overpass(query, str(tmp))
        _overpass_to_geojson(tmp, Path(out_path))
        return Path(out_path)



async def get_rural_gminas(self, voivodeship_terc: str = "28"):
    v_id = await self.fetch_voivodeship_relation_id(voivodeship_terc)
    
    # 2. Pobierz wszystkie gminy
    all_gminas = await self.fetch_gminas_in_voivodeship(v_id)
    
    rural_gminas = []
    for g in all_gminas:
        terc = g["tags"].get("teryt:terc", "")
        if terc.endswith("2") or terc.endswith("5"):
            rural_gminas.append(g)
            
    return rural_gminas