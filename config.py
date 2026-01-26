from dotenv import load_dotenv
import os
from pathlib import Path

class Config:
    load_dotenv()
    """Centralne adresy API i domyślne ustawienia."""
    BASE_DIR = Path.absolute(Path(__file__).parent)

    # OSM / Overpass
    OVERPASS_API_URL = "https://overpass-api.de/api/interpreter"

    DEFAULT_TIMEOUT = 60

    # Geokodowanie / bbox miast (Nominatim)
    NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
    NOMINATIM_USER_AGENT = "communication-exclusion/1.0"
    
    # Filtr gmin wiejskich (OSM + TERYT)
    # TERC: ostatnia cyfra typu gminy; 2 = gmina wiejska (najczęściej)
    WARMINSKO_MAZURSKIE_TERC = "28"
    RURAL_GMINA_TERC_SUFFIXES = {"2"}


