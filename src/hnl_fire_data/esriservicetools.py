import requests

__version__ = '0.1'

def print_hello():
    print("Hello World!")

def explore_service(url, servicelayer=0):
    """
    Explore the ArcGIS service to understand its structure
    """
    print("Exploring ArcGIS service structure...")
    
    try:
        response = requests.get(f"{url}?f=json")
        response.raise_for_status()
        service_info = response.json()
        
        print(f"Service Name: {service_info.get('serviceDescription', 'N/A')}")
        print(f"Layers: {len(service_info.get('layers', []))}")
        
        for layer in service_info.get('layers', []):
            print(f"  Layer {layer['id']}: {layer['name']}")
        
        # Get layer 0 details
        layer_url = f"{url}/{servicelayer}"
        response = requests.get(f"{layer_url}?f=json")
        response.raise_for_status()
        layer_info = response.json()
        
        print(f"\nLayer {servicelayer} Details:")
        print(f"  Name: {layer_info.get('name', 'N/A')}")
        print(f"  Geometry Type: {layer_info.get('geometryType', 'N/A')}")
        
        # Show available fields
        fields = layer_info.get('fields', [])
        print(f"  Fields ({len(fields)}):")
        for field in fields:
            print(f"    - {field['name']} ({field['type']})")
        
        # Try a simple query to test connectivity
        query_url = f"{layer_url}/query"
        test_params = {
            'where': '1=1',
            'returnCountOnly': 'true',
            'f': 'json'
        }
        response = requests.get(query_url, params=test_params)
        response.raise_for_status()
        count_result = response.json()
        if 'count' in count_result:
            print(f"  Total records in service: {count_result['count']}")
        return layer_info
        
    except Exception as e:
        print(f"Error exploring service: {e}")
        return None
