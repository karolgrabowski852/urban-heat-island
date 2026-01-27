# Communication Exclusion — analiza dostępności komunikacyjnej

Repozytorium do analizy wykluczenia komunikacyjnego (Polska, województwo warmińsko-mazurskie).

## Zawartość projektu
- `main.py` — główny skrypt (punkt wejścia)
- `data_pipeline.py`, `fetchers.py`, `transit_accessibility_analysis.py` — moduły przetwarzania danych i analiz
- `run_all_gminas_service_area.py` — uruchamia obliczenia dla wszystkich gmin
- `run_single_gmina_service_area.py` — uruchamia obliczenia dla pojedynczej gminy
- `config.py` — konfiguracja projektu
- `data/` — surowe i tymczasowe dane (geojson, pliki GDB itp.)
- `output/` — wyniki, np. `wyniki_wszystkie_gminy.csv` i `service_area.gdb`

## Wymagania
- Python 3.9+ (zalecane 3.10/3.11)
- Zainstaluj zależności:

```bash
pip install -r requirements.txt
```


## Szybkie uruchomienie
- Uruchom główny skrypt:

```bash
python main.py
```

- Przykładowo uruchom wszystkie gminy:

```bash
python run_all_gminas_service_area.py
```

- Uruchom analizę dla pojedynczej gminy:

```bash
python run_single_gmina_service_area.py
```

## Struktura danych i wyniki
- Wejście: pliki w `data/` (geojson, pliki tymczasowe gmin, warstwy GDB)
- Wyjście: katalog `output/` zawiera pliki CSV oraz geobaze `service_area.gdb` z obliczonymi zasięgami i metrykami dostępności

## Uwagi
- Uruchamiaj w środowisku, które ma dostęp do ArcGIS Python.

