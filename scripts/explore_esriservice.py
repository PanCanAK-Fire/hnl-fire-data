from hnl_fire_data import esriservicetools as et

# Get service info
# SERVICE_URL = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/AK_Recorded_Lightning/FeatureServer"

SERVICE_URL = "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/FireHistory/FeatureServer"
LAYER = 1

def main():
    """
    Main function to retrieve layer data
    """
    print("Alaska Lightning Data Retrieval Tool")
    print("=" * 40)
    
    # First explore the service to understand its structure
    layer_info = et.explore_service(url=SERVICE_URL, servicelayer=LAYER)
    # layer_info = explore_service()
    
    if layer_info is None:
        print("Could not access the service. Please check the URL and try again.")
        return
    
    print("\n" + "=" * 40)

if __name__ == "__main__":
    main()
    