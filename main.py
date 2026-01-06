"""
Skrypt ArcPy do analizy Miejskiej Wyspy Ciepła (UHI).

Zakres:
- konwersja rastra termalnego Landsat 8 (Band 10) do temperatury (°C),
- wygenerowanie siatki fishnet 100 m,
- średnia temperatura w oczkach (Zonal Statistics as Table),
- udział powierzchni zieleni (Tabulate Intersection),
- scalanie wyników i eksport do GDB,
- obsługa błędów arcpy.ExecuteError i sprawdzanie licencji Spatial Analyst.

Autor: Copilot
"""

import argparse
import logging
import os
from typing import Tuple

import arcpy
from arcpy import sa


# Stałe kalibracyjne dla Landsat 8 Band 10 (domyślne; można nadpisać parametrami)
DEFAULT_ML = 0.0003342  # multiplicative rescaling factor (ML)
DEFAULT_AL = 0.1        # additive rescaling factor (AL)
DEFAULT_K1 = 774.8853   # K1 Constant (W/(m^2 * sr * µm))
DEFAULT_K2 = 1321.0789  # K2 Constant (Kelvin)


def setup_logging() -> None:
	logging.basicConfig(
		level=logging.INFO,
		format="%(asctime)s [%(levelname)s] %(message)s",
	)


def check_spatial_analyst_license() -> None:
	status = arcpy.CheckExtension("Spatial")
	if status != "Available":
		raise RuntimeError("Wymagana licencja Spatial Analyst jest niedostępna.")
	arcpy.CheckOutExtension("Spatial")


def ensure_gdb(path: str) -> str:
	if os.path.exists(path):
		return path
	folder, gdb_name = os.path.split(path)
	if not folder:
		folder = os.getcwd()
	logging.info("Tworzę GDB: %s", path)
	arcpy.management.CreateFileGDB(folder, gdb_name)
	return path


def calc_temperature_celsius(
	thermal_raster: str,
	ml: float,
	al: float,
	k1: float,
	k2: float,
) -> sa.Raster:
	"""Przelicza raster DN na temperaturę powierzchni (°C).

	Wzory: Lλ = ML * Qcal + AL;  T = K2 / ln((K1 / Lλ) + 1) - 273.15
	"""

	logging.info("Przeliczam DN -> radiancja -> temperatura (°C)")
	radiance = sa.Raster(thermal_raster) * ml + al
	temp_c = (k2 / sa.Ln((k1 / radiance) + 1)) - 273.15
	return temp_c


def create_fishnet(
	extent_fc: str,
	out_fishnet: str,
	cell_size: float = 100.0,
) -> str:
	"""Tworzy siatkę kwadratową (100 m) dopasowaną do zasięgu extent_fc."""

	desc = arcpy.Describe(extent_fc)
	ext = desc.extent
	origin_coord = f"{ext.XMin} {ext.YMin}"
	y_axis_coord = f"{ext.XMin} {ext.YMin + 10}"
	corner_coord = f"{ext.XMax} {ext.YMax}"

	logging.info("Tworzę fishnet: %s", out_fishnet)
	arcpy.management.CreateFishnet(
		out_feature_class=out_fishnet,
		origin_coord=origin_coord,
		y_axis_coord=y_axis_coord,
		cell_width=cell_size,
		cell_height=cell_size,
		number_rows="0",
		number_columns="0",
		corner_coord=corner_coord,
		labels="NO_LABELS",
		template=extent_fc,
		geometry_type="POLYGON",
	)

	clipped = arcpy.management.Clip(
		in_features=out_fishnet,
		clip_features=extent_fc,
		out_feature_class=f"{out_fishnet}_clip",
	).getOutput(0)
	return clipped


def zonal_stats_mean(
	zones_fc: str,
	value_raster: sa.Raster,
	out_table: str,
) -> str:
	oid_field = arcpy.Describe(zones_fc).OIDFieldName
	logging.info("Liczenie średniej temperatury w oczkach (Zonal Statistics)")
	sa.ZonalStatisticsAsTable(
		in_zone_data=zones_fc,
		zone_field=oid_field,
		in_value_raster=value_raster,
		out_table=out_table,
		statistics_type="MEAN",
		ignore_nodata="DATA",
	)
	return out_table


def tabulate_green(
	zones_fc: str,
	green_fc: str,
	out_table: str,
) -> Tuple[str, str]:
	oid_field = arcpy.Describe(zones_fc).OIDFieldName
	logging.info("Obliczam udział zieleni (Tabulate Intersection)")
	arcpy.analysis.TabulateIntersection(
		in_zone_features=zones_fc,
		zone_fields=oid_field,
		in_class_features=green_fc,
		out_table=out_table,
		out_units="SQUARE_METERS",
	)
	return out_table, oid_field


def join_results(
	fishnet_fc: str,
	zonal_table: str,
	tab_table: str,
	oid_field: str,
) -> str:
	logging.info("Dodaję średnią temperaturę do siatki")
	arcpy.management.JoinField(
		fishnet_fc,
		oid_field,
		zonal_table,
		oid_field,
		["MEAN"],
	)

	logging.info("Dołączam metryki zieleni")
	arcpy.management.JoinField(
		fishnet_fc,
		oid_field,
		tab_table,
		oid_field,
		["AREA", "PERCENTAGE"],
	)

	logging.info("Wyliczam pole powierzchni oczka i % zieleni")
	arcpy.management.CalculateGeometryAttributes(
		fishnet_fc,
		[["cell_area", "AREA"]],
		area_unit="SQUARE_METERS",
	)

	if "PERCENTAGE" not in [f.name for f in arcpy.ListFields(fishnet_fc)]:
		arcpy.management.AddField(fishnet_fc, "PERCENTAGE", "DOUBLE")

	arcpy.management.AddField(fishnet_fc, "pct_green", "DOUBLE")
	arcpy.management.CalculateField(
		fishnet_fc,
		"pct_green",
		"(!AREA! / !cell_area!) * 100 if !cell_area! > 0 else None",
		expression_type="PYTHON3",
	)

	return fishnet_fc


def export_result(fishnet_fc: str, out_gdb: str, out_name: str) -> str:
	ensure_gdb(out_gdb)
	out_path = os.path.join(out_gdb, out_name)
	logging.info("Eksportuję wynik do: %s", out_path)
	arcpy.management.CopyFeatures(fishnet_fc, out_path)
	return out_path


def parse_args() -> argparse.Namespace:
	parser = argparse.ArgumentParser(description="Analiza UHI z wykorzystaniem ArcPy")
	parser.add_argument("thermal_raster", help="Raster termalny Landsat 8 Band 10 (DN)")
	parser.add_argument("green_fc", help="Warstwa zieleni BDOT10k (poligony)")
	parser.add_argument("city_boundary", help="Granica miasta (PRG) do wyznaczenia zasięgu")
	parser.add_argument("out_gdb", help="Ścieżka do geobazy wynikowej (utworzona, jeśli brak)")
	parser.add_argument(
		"--fishnet-name",
		default="uhi_fishnet",
		help="Nazwa klasy obiektów fishnet w GDB",
	)
	parser.add_argument("--cell-size", type=float, default=100.0, help="Wielkość oczka siatki (m)")
	parser.add_argument("--ml", type=float, default=DEFAULT_ML, help="ML - multiplicative factor")
	parser.add_argument("--al", type=float, default=DEFAULT_AL, help="AL - additive factor")
	parser.add_argument("--k1", type=float, default=DEFAULT_K1, help="Stała K1")
	parser.add_argument("--k2", type=float, default=DEFAULT_K2, help="Stała K2")
	return parser.parse_args()


def main() -> None:
	setup_logging()
	args = parse_args()
	arcpy.env.overwriteOutput = True

	try:
		check_spatial_analyst_license()

		temp_c = calc_temperature_celsius(
			thermal_raster=args.thermal_raster,
			ml=args.ml,
			al=args.al,
			k1=args.k1,
			k2=args.k2,
		)

		fishnet = create_fishnet(
			extent_fc=args.city_boundary,
			out_fishnet=os.path.join("in_memory", "fishnet"),
			cell_size=args.cell_size,
		)

		zonal_table = os.path.join("in_memory", "zonal_mean")
		zonal_stats_mean(fishnet, temp_c, zonal_table)

		tab_table = os.path.join("in_memory", "tab_green")
		tab_table, oid_field = tabulate_green(fishnet, args.green_fc, tab_table)

		joined_fc = join_results(fishnet, zonal_table, tab_table, oid_field)

		export_result(joined_fc, args.out_gdb, args.fishnet_name)
		logging.info("Zakończono analizę UHI")

	except arcpy.ExecuteError:
		logging.error("Błąd ArcPy: %s", arcpy.GetMessages(2))
		sys.exit(1)
	except Exception as exc:
		logging.exception("Nieoczekiwany błąd: %s", exc)
		sys.exit(1)


if __name__ == "__main__":
	main()