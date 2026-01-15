import asyncio
from pathlib import Path

from config import Config
from fetchers import Fetcher

class OSM:
    def get_data(self, out_dir, city, **kwargs):
        out_dir = Path(out_dir)

        asyncio.run(
            self.download_city_data(
                out_dir=out_dir,
                city=city,
                country=kwargs.get("country"),
                buffer_km=kwargs.get("buffer_km", Config.DEFAULT_BUFFER_KM),
                year=kwargs.get("year", 2023),
            )
        )

    async def download_city_data(
        self,
        out_dir: Path,
        city: str,
        country: str | None,
        buffer_km: float,
        year: int,
    ) -> None:

        stops_path = out_dir / "osm_stops.geojson"
        walk_path = out_dir / "osm_walk_network.geojson"
        buildings_path = out_dir / "bdot_buildings.geojson"

        out_dir.mkdir(parents=True, exist_ok=True)

        async with Fetcher() as f:
            bbox = await f.fetch_city_bbox(city=city, country=country, buffer_km=buffer_km)
            
            await f.download_osm_transit_stops(bbox, str(stops_path))
            await f.download_osm_walk_network(bbox, str(walk_path))
            await f.download_osm_buildings(bbox, str(buildings_path).replace("bdot", "osm"))
            # await f.download_bdl_population(Config.BDL_LUBLIN_UNIT, year, str(bdl_path))

OSM().get_data(out_dir="data/puławy", city="Puławy", country="Polska", buffer_km=1.0)
