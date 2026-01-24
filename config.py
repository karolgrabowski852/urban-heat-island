from dotenv import load_dotenv
import os
class Config:
    load_dotenv()
    """Centralne adresy API i domyślne ustawienia."""

    # OSM / Overpass
    OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"
    OVERPASS_API_MIRROR = "https://lz4.overpass-api.de/api/interpreter"
    OVERPASS_API_ALT = "https://overpass.kumi.systems/api/interpreter"
    OSM_BOUNDARIES_URL = "https://osm-boundaries.com/"

    # Geoportal WFS (PRG + BDOT10k)
    # Note: GEO_BDOT10K_WFS often returns 401. Consider using OSM or downloading data manually from Geoportal for "First Mile".
    GEO_BDOT10K_WFS = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/BDOT10k/WFS/Skorowidze"
    GEO_PRG_WFS = "https://mapy.geoportal.gov.pl/wss/service/PRG/WFS"
    BDOT10K_BUILDING_LAYER = "OT_BUBD_A"

    # GUS BDL (Bank Danych Lokalnych)
    BDL_API_BASE = "https://bdl.stat.gov.pl/api/v1"
    BDL_POPULATION_VAR = "60547"  # liczba ludności ogółem (ID zmiennej BDL)
    BDL_LUBLIN_UNIT = "0663000"  # TERYT Lublin (miasto na prawach powiatu, 12-cyfrowy)
    BDL_CLIENT_ID = os.getenv("BDL_CLIENT_ID")

    # Landsat otwarte źródła
    EARTH_SEARCH_STAC = "https://earth-search.aws.element84.com/v1"  # STAC API (open)
    LANDSATLOOK_STAC = "https://landsatlook.usgs.gov/stac-server"   # STAC API LandsatLook
    LANDSAT_AWS_HTTP = "https://landsatlook.usgs.gov/data"  # publiczne HTTP do plików

    # Domyślne nagłówki i timeouty
    DEFAULT_TIMEOUT = 60

    # Geokodowanie / bbox miast (Nominatim)
    NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
    NOMINATIM_USER_AGENT = "urban-heat-island/1.0"

    # Domyślne ścieżki / parametry lokalne
    DEFAULT_LUBLIN_BOUNDARY = "data/lublin/lublin_boundary.geojson"
    DEFAULT_BUFFER_KM = 1.0
    BDOT10K_BUILDING_LAYER = "BDOT10k:BuildingA"  # można nadpisać jeśli Geoportal ma inną nazwę

    # Administracyjne: województwo warmińsko-mazurskie (OSM)
    WARMINSKO_MAZURSKIE_VOIVODESHIP = "Warmińsko-Mazurskie"
    WARMINSKO_MAZURSKIE_TERC = "28"

    # Filtr gmin wiejskich (OSM + TERYT)
    # TERC: ostatnia cyfra typu gminy; 2 = gmina wiejska (najczęściej)
    RURAL_GMINA_TERC_SUFFIXES = {"2"}

