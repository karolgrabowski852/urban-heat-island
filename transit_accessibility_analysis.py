import arcpy
from arcpy import sa
from arcpy import management
from arcpy import na
import os
import logging
import json

class Workspace:
    def __init__(self, workspace_gdb: str, logging_level=logging.INFO):
        """Initialize workspace settings and ArcPy environment."""
        self.workspace = workspace_gdb
        self.setup_logging(logging_level)
        self.ensure_workspace()
        
        self.sr_metric = arcpy.SpatialReference(2180)
        self.sr_wgs84 = arcpy.SpatialReference(4326)
        
        self.walking_speed_kmh = 5.0
        self.cell_size = 3.0
        
    def setup_logging(self, level):
        """Configure module logger and logging level."""
        logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
        self.logger = logging.getLogger(__name__)

    def ensure_workspace(self):
        """Ensure the file geodatabase exists and set ArcPy env."""
        folder, name = os.path.split(self.workspace)
        if not folder:
            folder = os.getcwd()
        if not arcpy.Exists(self.workspace):
            self.logger.info(f"Creating GDB: {self.workspace}")
            management.CreateFileGDB(folder, name)
        arcpy.env.workspace = self.workspace
        arcpy.env.overwriteOutput = True

    def import_geojson(self, input_path: str, output_name: str, target_sr=None, geometry_type_override=None):
        """Import GeoJSON into a GDB feature class using InsertCursor."""
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
    
class ArcGisPipeline(Workspace):
    def build_network_dataset_from_roads(
        self,
        road_geojson: str,
        feature_dataset_name: str = "network_fd",
        network_name: str = "road_nd",
        edge_fc_name: str = "road_edges",
    ) -> str:
        """Build a road network dataset (all road types) with length-based impedance at 5.0 km/h walking speed."""
        if not arcpy.CheckExtension("Network"):
            raise Exception("Network Analyst license required.")
        arcpy.CheckOutExtension("Network")

        try:
            road_fc = self.import_geojson(road_geojson, "road_net_lines")

            fd_path = os.path.join(self.workspace, feature_dataset_name)
            nd_path = os.path.join(fd_path, network_name)
            edge_fc_path = os.path.join(fd_path, edge_fc_name)

            if arcpy.Exists(nd_path):
                try:
                    arcpy.management.Delete(nd_path)
                except Exception:
                    pass

            if arcpy.Exists(fd_path):
                try:
                    arcpy.management.Delete(fd_path)
                except Exception:
                    pass

            if not arcpy.Exists(fd_path):
                arcpy.management.CreateFeatureDataset(self.workspace, feature_dataset_name, self.sr_metric)

            root_edge_fc = os.path.join(self.workspace, edge_fc_name)
            if arcpy.Exists(root_edge_fc):
                try:
                    arcpy.management.Delete(root_edge_fc)
                except Exception:
                    pass

            arcpy.conversion.FeatureClassToFeatureClass(road_fc, fd_path, edge_fc_name)

            # Add a walking time field (minutes) for reference
            if "walk_time_min" not in [f.name for f in arcpy.ListFields(edge_fc_path)]:
                arcpy.management.AddField(edge_fc_path, "walk_time_min", "DOUBLE")
            meters_per_min = (self.walking_speed_kmh * 1000.0) / 60.0
            arcpy.management.CalculateField(
                edge_fc_path,
                "walk_time_min",
                f"!shape.length@meters! / {meters_per_min}",
                "PYTHON3",
            )

            na.CreateNetworkDataset(fd_path, network_name, [edge_fc_name], "NO_ELEVATION")
            na.BuildNetwork(nd_path)

            return nd_path

        finally:
            arcpy.CheckInExtension("Network")



    def run_service_area(
        self,
        network_dataset: str,
        stops_geojson: str,
        boundary_geojson: str | None = None,
        travel_mode: str | None = None,
        cutoffs: list[float] | None = None,
        output_polygons_name: str = "service_area_polygons",
    ) -> str:
        """Run Network Analyst Service Area to generate isochrone polygons."""
        if not arcpy.CheckExtension("Network"):
            raise Exception("Network Analyst license required.")
        arcpy.CheckOutExtension("Network")

        try:
            stops_fc = self.import_geojson(stops_geojson, "stops_pts")

            boundary_fc = None
            if boundary_geojson:
                boundary_fc = self.import_geojson(boundary_geojson, "boundary_mask", geometry_type_override="POLYGON")

            if not cutoffs:
                cutoffs = [5, 10, 15]

            # Create Service Area layer
            travel_mode_val = travel_mode if travel_mode else ""

            sa_layer = na.MakeServiceAreaAnalysisLayer(
                network_dataset,
                "ServiceArea",
                travel_mode_val,
                "FROM_FACILITIES",
                cutoffs,
                None,
                "LOCAL_TIME_AT_LOCATIONS",
                "POLYGONS",
                "STANDARD",
                "DISSOLVE",
                "RINGS",
            )[0]

            sublayers = na.GetNAClassNames(sa_layer)
            facilities_layer = sublayers.get("Facilities")
            polygons_layer = sublayers.get("SAPolygons")

            na.AddLocations(
                in_network_analysis_layer=sa_layer,
                sub_layer=facilities_layer,
                in_table=stops_fc,
                search_tolerance="500 Meters",
                append="CLEAR",
            )

            na.Solve(sa_layer)

            out_polygons = os.path.join(self.workspace, output_polygons_name)
            if arcpy.Exists(out_polygons):
                arcpy.management.Delete(out_polygons)

            arcpy.management.CopyFeatures(polygons_layer, out_polygons)

            self.logger.info(f"Service Area complete. Result: {out_polygons}")
            return out_polygons

        except Exception as e:
            self.logger.error(f"Service Area failed: {e}")
            raise
        finally:
            arcpy.CheckInExtension("Network")

    def calculate_building_access(self, buildings_geojson: str, service_area_polygons: str) -> str:
        """Calculate residential building access to service area polygons."""
        self.logger.info("Calculating building access to Service Area...")

        bldgs_fc = self.import_geojson(buildings_geojson, "buildings_polys", geometry_type_override="POLYGON")

        # Filter residential buildings
        res_layer = "res_layer"
        if arcpy.Exists(res_layer):
            arcpy.management.Delete(res_layer)
        where_clause = "building IN ('apartments', 'house', 'residential', 'detached', 'semidetached_house', 'terrace', 'dormitory', 'yes')"
        arcpy.management.MakeFeatureLayer(bldgs_fc, res_layer, where_clause)

        res_count = int(arcpy.management.GetCount(res_layer).getOutput(0))
        self.logger.info(f"Found {res_count} residential buildings.")

        # Spatial Join: buildings to service area polygons (JOIN_ONE_TO_ONE with INTERSECT)
        joined_fc = os.path.join(self.workspace, "buildings_with_access")
        if arcpy.Exists(joined_fc):
            arcpy.management.Delete(joined_fc)

        arcpy.analysis.SpatialJoin(
            target_features=res_layer,
            join_features=service_area_polygons,
            out_feature_class=joined_fc,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL",
            match_option="INTERSECT",
        )

        # Add has_access field: 1 if Join_Count > 0 (has ServiceArea polygon), else 0
        if "has_access" not in [f.name for f in arcpy.ListFields(joined_fc)]:
            arcpy.management.AddField(joined_fc, "has_access", "SHORT")

        code_block = """
def mark_access(join_count):
    return 1 if join_count and join_count > 0 else 0
"""
        arcpy.management.CalculateField(
            joined_fc,
            "has_access",
            "mark_access(!Join_Count!)",
            "PYTHON3",
            code_block,
        )

        # Calculate statistics
        with arcpy.da.SearchCursor(joined_fc, ["has_access"]) as cursor:
            access_count = sum(1 for row in cursor if row[0] == 1)

        access_pct = (access_count / res_count * 100) if res_count > 0 else 0
        self.logger.info(
            f"Building access results: {access_count}/{res_count} buildings ({access_pct:.1f}%) have access to Service Area."
        )

        return joined_fc, access_pct
