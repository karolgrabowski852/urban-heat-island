from data_pipeline import OSM
from run_all_gminas_service_area import main


if __name__ == "__main__":
    OSM().get_rural_gminas_data(out_dir="data/warmia-mazury-rural")
    main()