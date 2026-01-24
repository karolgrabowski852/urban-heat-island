import arcpy
from arcpy import sa
from arcpy import management
import os
import logging
import json

class Workspace:
    def __init__(self, workspace_gdb: str, logging_level=logging.INFO):
        self.workspace = workspace_gdb
        self.setup_logging(logging_level)
        self.ensure_workspace()
        
        self.sr_metric = arcpy.SpatialReference(2180)
        self.sr_wgs84 = arcpy.SpatialReference(4326)
        
        self.walking_speed_kmh = 5.0
        self.cell_size = 3.0
        
    def setup_logging(self, level):
        logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
        self.logger = logging.getLogger(__name__)

    def ensure_workspace(self):
        folder, name = os.path.split(self.workspace)
        if not folder:
            folder = os.getcwd()
        if not arcpy.Exists(self.workspace):
            self.logger.info(f"Creating GDB: {self.workspace}")
            management.CreateFileGDB(folder, name)
        arcpy.env.workspace = self.workspace
        arcpy.env.overwriteOutput = True

    def import_geojson(self, input_path: str, output_name: str, target_sr=None, geometry_type_override=None):
        """Custom import from GeoJSON to GDB Feature Class using InsertCursor."""
        self.logger.info(f"Importing {input_path} to {output_name}")
        
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        features = data.get("features", [])
        if not features:
            raise ValueError(f"No features in {input_path}")
            
        if geometry_type_override:
            geom_type = geometry_type_override
        else:
            first_geom = features[0]["geometry"]["type"]
            if first_geom == "Point":
                geom_type = "POINT"
            elif first_geom == "LineString":
                geom_type = "POLYLINE"
            elif first_geom in ["Polygon", "MultiPolygon"]:
                geom_type = "POLYGON"
            else:
                geom_type = "POINT"
            
        temp_fc_name = f"temp_cust_{output_name}"
        temp_fc = os.path.join(self.workspace, temp_fc_name)
        
        if arcpy.Exists(temp_fc):
            arcpy.management.Delete(temp_fc)
            
        arcpy.management.CreateFeatureclass(self.workspace, temp_fc_name, geom_type, spatial_reference=self.sr_wgs84)
        
        arcpy.management.AddField(temp_fc, "name", "TEXT", field_length=255)
        arcpy.management.AddField(temp_fc, "highway", "TEXT", field_length=50)
        arcpy.management.AddField(temp_fc, "building", "TEXT", field_length=50)
        
        fields = ["SHAPE@", "name", "highway", "building"]
        
        inserted_count = 0
        with arcpy.da.InsertCursor(temp_fc, fields) as cursor:
            for feat in features:
                props = feat.get("properties", {})
                geom = feat.get("geometry", {})
                if not geom: continue
                
                g_type = geom.get("type")
                coords = geom.get("coordinates")
                
                try:
                    shape_list = []
                    if geom_type == "POINT" and g_type == "Point":
                        shape_list.append(arcpy.PointGeometry(arcpy.Point(coords[0], coords[1]), self.sr_wgs84))
                    
                    elif geom_type == "POLYLINE" and g_type == "LineString":
                        array = arcpy.Array([arcpy.Point(c[0], c[1]) for c in coords])
                        shape_list.append(arcpy.Polyline(array, self.sr_wgs84))
                        
                    elif geom_type == "POLYGON":
                        bg_polys = []
                        if g_type == "Polygon":
                            bg_polys = [coords] 
                        elif g_type == "MultiPolygon":
                            bg_polys = coords
                        elif g_type == "LineString":
                             # Convert closed LineString to Polygon
                            if coords and len(coords) > 2 and coords[0] == coords[-1]:
                                bg_polys = [[coords]] # Wrap as Polygon structure [[outer_ring]]
                            else:
                                # Try to close it if valid building? Or just skip open lines.
                                # Assuming OSM buildings as lines are usually closed rings
                                coords_closed = list(coords)
                                coords_closed.append(coords[0])
                                bg_polys = [[coords_closed]]
                        
                        for poly_geom in bg_polys:
                            outer_ring = poly_geom[0]
                            array = arcpy.Array([arcpy.Point(c[0], c[1]) for c in outer_ring])
                            poly = arcpy.Polygon(array, self.sr_wgs84)
                            if poly and not poly.isMultipart: # simple check
                                shape_list.append(poly)
                        
                    for shape in shape_list:
                        if shape:
                            building_val = str(props.get("building", "") or "")[:50]
                            cursor.insertRow([shape, str(props.get("name", "") or "")[:255], str(props.get("highway", "") or "")[:50], building_val])
                            inserted_count += 1
                except Exception:
                    continue

        self.logger.info(f"Inserted {inserted_count} features (attempted {len(features)}) into {temp_fc}. Geometry: {geom_type}")
        
        if inserted_count == 0:
            self.logger.warning(f"Warning: 0 features inserted info {temp_fc}")

        sr = target_sr if target_sr else self.sr_metric
        
        out_fc = os.path.join(self.workspace, output_name)
        if arcpy.Exists(out_fc):
            arcpy.management.Delete(out_fc)

        management.Project(temp_fc, out_fc, sr)
        return out_fc



    def create_impedance_raster(self, walk_network_fc: str, study_area_mask: str) -> str:
        """
        Creates a cost surface raster.
        - Walk network: Low cost (walking speed)
        - Off-network: High cost (100x) to simulate "sidewalks only" but allow door-to-street connections.
        """
        self.logger.info("Creating impedance raster...")
        
        # 1. Prepare Inputs and Extent
        desc_study = arcpy.Describe(study_area_mask)
        study_extent = desc_study.extent
        
        background_cost = 10.0 # High cost for "exclusion" analysis
        network_cost = 1.0
        
        arcpy.env.extent = study_extent
        arcpy.env.cellSize = self.cell_size
        
        # 2. Create Background Raster (Value=100) covering the entire extent
        self.logger.info("Generating background cost raster...")
        bg_extent_fc = os.path.join(self.workspace, "bg_extent_poly")
        
        # Try to delete if exists, but handle locking via rename
        if arcpy.Exists(bg_extent_fc):
            arcpy.management.Delete(bg_extent_fc)
            
        pts = [
            arcpy.Point(study_extent.XMin, study_extent.YMin),
            arcpy.Point(study_extent.XMin, study_extent.YMax),
            arcpy.Point(study_extent.XMax, study_extent.YMax),
            arcpy.Point(study_extent.XMax, study_extent.YMin), 
            arcpy.Point(study_extent.XMin, study_extent.YMin)
        ]
        array = arcpy.Array(pts)
        extent_poly = arcpy.Polygon(array, desc_study.spatialReference)
        
        arcpy.management.CreateFeatureclass(self.workspace, "bg_extent_poly", "POLYGON", spatial_reference=desc_study.spatialReference)
        arcpy.management.AddField(bg_extent_fc, "COST", "FLOAT")
        
        with arcpy.da.InsertCursor(bg_extent_fc, ["SHAPE@", "COST"]) as cur:
            cur.insertRow([extent_poly, background_cost])
            
        bg_grid = "background_grid"
        arcpy.conversion.PolygonToRaster(
            in_features=bg_extent_fc, 
            value_field="COST", 
            out_rasterdataset=bg_grid, 
            cellsize=self.cell_size
        )
        bg_ras = sa.Raster(bg_grid)

        # 3. Process Walk Network
        desc_walk = arcpy.Describe(walk_network_fc)
        input_lines = walk_network_fc
        
        if desc_walk.shapeType != "Polyline":
            # Conversion logic if needed (borrowed from previous version)
            converted_lines = os.path.join(self.workspace, "walk_lines_fixed")
            if arcpy.Exists(converted_lines): arcpy.management.Delete(converted_lines)
            if desc_walk.shapeType == "Polygon": arcpy.management.PolygonToLine(walk_network_fc, converted_lines)
            else: arcpy.management.FeatureToLine(walk_network_fc, converted_lines)
            input_lines = converted_lines

        # Buffer the walk network
        buffered_lines = os.path.join(self.workspace, "walk_lines_buffered")
        if arcpy.Exists(buffered_lines):
            arcpy.management.Delete(buffered_lines)
            
        self.logger.info("Buffering walk network (2.5m)...")
        arcpy.analysis.Buffer(input_lines, buffered_lines, "2.5 Meters")
        
        arcpy.management.AddField(buffered_lines, "COST", "FLOAT")
        arcpy.management.CalculateField(buffered_lines, "COST", network_cost, "PYTHON3")

        # Rasterize Network (Value=1)
        walk_grid = "walk_network_grid"
        arcpy.conversion.PolygonToRaster(
            in_features=buffered_lines,
            value_field="COST", 
            out_rasterdataset=walk_grid,
            cell_assignment="CELL_CENTER",
            cellsize=self.cell_size
        )

        walk_ras = sa.Raster(walk_grid)
        impedance_raster = sa.CellStatistics([bg_ras, walk_ras], "MINIMUM", "DATA")
        
        impedance_output = "impedance_surface"
        impedance_raster.save(impedance_output)
        return impedance_output

    def calculate_travel_time(self, stops_fc: str, impedance_raster: str, max_time_min: int = 15) -> str:
        """
        Calculates travel time (minutes) from closest stop.
        Cost Distance returns 'cost units'.
        """
        self.logger.info("Calculating travel time surface...")
        
        speed_mps = (self.walking_speed_kmh * 1000) / 3600.0
        min_per_meter_network = (1.0 / speed_mps) / 60.0
        
        base_imp = sa.Raster(impedance_raster)
        
        cost_surface_vals = base_imp * min_per_meter_network
        
        travel_time_ras = sa.CostDistance(
            in_source_data=stops_fc,
            in_cost_raster=cost_surface_vals,
            maximum_distance=None 
        )
        
        out_name = "travel_time_min"
        travel_time_ras.save(out_name)
        return out_name

    def classify_isochrones(self, travel_time_raster: str) -> str:
        """Classify into bins: <5, 5-10, >10 (White Spots are >10)"""
        self.logger.info("Classifying isochrones...")
        
        remap = sa.RemapRange([
            [0, 5, 1],
            [5, 10, 2],
            [10, 99999, 3]
        ])
        
        iso_ras = sa.Reclassify(travel_time_raster, "Value", remap)
        out_name = "isochrones_class"
        iso_ras.save(out_name)
        return out_name

    def calculate_building_travel_times(self, buildings_fc: str, travel_time_raster: str) -> str:
        """
        Calculates specific travel time for every residential building.
        1. Filters residential buildings.
        2. Converts to INSIDE centroids.
        3. Extracts Raster Value.
        4. Classifies as Excluded if > 15 min.
        """
        self.logger.info("Calculating per-building travel times...")
        
        res_layer = "res_layer"
        if arcpy.Exists(res_layer): arcpy.management.Delete(res_layer)
        where_clause = "building IN ('apartments', 'house', 'residential', 'detached', 'semidetached_house', 'terrace', 'dormitory', 'yes')"
        arcpy.management.MakeFeatureLayer(buildings_fc, res_layer, where_clause)
        
        count = int(arcpy.management.GetCount(res_layer).getOutput(0))
        self.logger.info(f"Analysis will run on {count} residential buildings.")

        centroids = os.path.join(self.workspace, "building_centroids_time")
        if arcpy.Exists(centroids): arcpy.management.Delete(centroids)
        management.FeatureToPoint(res_layer, centroids, "INSIDE")

        self.logger.info("Extracting travel time values to centroids...")
        sa.ExtractMultiValuesToPoints(centroids, [[travel_time_raster, "walk_time_min"]], "NONE")
        
        code_block = """
def classify(time_val):
    if time_val is None: return 1 # Excluded (unreachable)
    if time_val > 15: return 1    # Excluded
    return 0                      # Included
"""
        arcpy.management.AddField(centroids, "is_excluded", "SHORT")
        arcpy.management.CalculateField(centroids, "is_excluded", "classify(!walk_time_min!)", "PYTHON3", code_block)
        
        return centroids

    def run_analysis(self, 
                     stops_geojson: str, 
                     walk_geojson: str, 
                     buildings_wfs_json: str, 
                     boundary_mask_fc: str = None
                     ):
        
        if not arcpy.CheckExtension("Spatial"):
            raise Exception("Spatial Analyst license required.")
        arcpy.CheckOutExtension("Spatial")

        try:
            stops_fc = self.import_geojson(stops_geojson, "stops_pts")
            walk_fc = self.import_geojson(walk_geojson, "walk_net_lines")
            blds_fc = self.import_geojson(buildings_wfs_json, "buildings_polys", geometry_type_override="POLYGON")
            
            if not boundary_mask_fc:
                study_area = walk_fc # Fallback
            else:
                study_area = boundary_mask_fc

            imp_surf = self.create_impedance_raster(walk_fc, study_area)
            
            time_surf = self.calculate_travel_time(stops_fc, imp_surf)
            
            bldg_points = self.calculate_building_travel_times(blds_fc, time_surf)
            
            self.logger.info(f"Analysis complete. Result: {bldg_points}")
            return bldg_points

        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            raise
        finally:
            arcpy.CheckInExtension("Spatial")


# if __name__ == "__main__":

#     BASE_DIR = os.getcwd()
#     DATA_DIR = os.path.join(BASE_DIR, "data", "puławy")
    
#     stops = os.path.join(DATA_DIR, "osm_stops.geojson")
#     walk = os.path.join(DATA_DIR, "osm_walk_network.geojson")
#     bldgs = os.path.join(DATA_DIR, "osm_buildings.geojson") 
    
#     gdb_path = os.path.join(BASE_DIR, "transport_analysis.gdb")
    
#     model = TransitAccessibilityModel(workspace_gdb=gdb_path)
    
#     if os.path.exists(stops) and os.path.exists(walk) and os.path.exists(bldgs):
#         print("Starting analysis...")
#         model.run_analysis(stops, walk, bldgs)
#     else:
#         print("Data not found. Run download pipeline first.")
