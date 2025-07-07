import requests
import json
import datetime as dt
import time
from pathlib import Path

TODAY = dt.datetime.now() 
TODAY_START = TODAY.replace(hour=0, minute=0, second=0, microsecond=0)
TODAY_END = TODAY.replace(hour=23, minute=59, second=59, microsecond=999999)
SERVICE_URL =  "https://fire.ak.blm.gov/arcgis/rest/services/MapAndFeatureServices/AK_Recorded_Lightning/FeatureServer/{0}/query"
DATASETS = {
    0: {'label': 'today', 'dayoffset': 0},
    1: {'label': 'yesterday', 'dayoffset': 1},
    2: {'label': 'two_days_ago', 'dayoffset': 2},
    3: {'label': 'three_days_ago', 'dayoffset': 3},
    4: {'label': 'last_two_weeks', 'dayoffset': 4},
}
SELECTED = [0, 1, 2, 3, 4]  # which datasets we want
AGGREGATE = True    # whether we want to aggregate the data into a single file 
AGG_FN = f"all_lightning_{TODAY.year}.fth"
OUTDIR = Path().absolute().parents[1] / "data/AICC_lightning"

def datadate(dsidx):
    """Get date of lightning data based on which service is accessed"""
    dayoffset = DATASETS[dsidx]['dayoffset']
    return TODAY_START - dt.timedelta(days=dayoffset)

def download_lightning_data(dsidx=0):
    """
    Retrieve lightning data from a particular time from Alaska BLM ArcGIS Feature Service
    """
    
    # Putting together tracking labels
    # Convert to epoch timestamps (milliseconds)
    
    lightningday = datadate(dsidx)
    print(f"Retrieving lightning data for: {lightningday.strftime('%Y-%m-%d')}")
    
    # Parameters for the query
    params = {
        'where': f"2=2",
        'outFields': '*',  # Get all attributes
        'returnGeometry': 'true',  # Include geometry (lat/lon)
        'f': 'json',  # Return format
        'resultRecordCount': 2000,  # Max records per request
        'resultOffset': 0  # Starting offset for pagination
    }
    url = SERVICE_URL.format(dsidx)
    all_features = []
    offset = 0
    
    while True:
        params['resultOffset'] = offset
        
        try:
            print(f"Fetching records starting at offset {offset}...")
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for errors in the response
            if 'error' in data:
                print(f"Error from ArcGIS service: {data['error']}")
                break
            
            features = data.get('features', [])
            
            if not features:
                print("No more records found.")
                break
            
            all_features.extend(features)
            print(f"Retrieved {len(features)} records (Total: {len(all_features)})")
            
            # Check if we got fewer records than requested (indicates end of data)
            if len(features) < params['resultRecordCount']:
                break
            
            offset += len(features)
            
            # Small delay to be respectful to the server
            time.sleep(0.5)
            
        except requests.exceptions.RequestException as e:
            print(f"Request error: {e}")
            break
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            break
    return all_features

def get_lightning_data(dsidx=0):
    """
    Retrieve and attach metadata
    """  
    features = download_lightning_data(dsidx=dsidx)
    lightningdate = datadate(dsidx)
    # Prepare data for saving
    output_data = {
        'metadata': {
            'total_records': len(features),
            'retrieved_date': TODAY.isoformat(),
            'data_date': lightningdate.isoformat(),
            'data_label': DATASETS[dsidx]['label'],
            'service_index': dsidx,
        },
        'features': features
    }
    return output_data


def save_data(features_with_metadata, filename=None):
    """
    Save the lightning data to a JSON file
    """
    label = features_with_metadata['metadata']['data_label']
    filename = f"alaska_lightning_{TODAY.strftime('%Y%m%d')}_{label}.json"

    with open(OUTDIR / filename, 'w') as f:
        json.dump(features_with_metadata, f, indent=2)
    print(f"Data saved to {filename}")
    return filename

def print_summary(features):
    """
    Print a summary of the retrieved data
    """
    if not features:
        print("No lightning data found for yesterday.")
        return
    
    print(f"\n=== LIGHTNING DATA SUMMARY ===")
    print(f"Total records: {len(features)}")
    
    # Sample the first record to show available attributes
    if features:
        sample_attrs = features[0].get('attributes', {})
        print(f"\nAvailable attributes:")
        for key in sample_attrs.keys():
            print(f"  - {key}")
        
        print(f"\nSample record:")
        print(f"  Attributes: {sample_attrs}")
        if 'geometry' in features[0]:
            geom = features[0]['geometry']
            print(f"  Geometry: {geom}")

def main():
    """
    Main function to retrieve and process lightning data
    """
    print("Alaska Lightning Data Retrieval Tool")
    print("=" * 40)
    
    # Retrieve the data
    for idx in SELECTED:
        features_with_metadata = get_lightning_data(idx)
    
    # Print summary
        print_summary(features_with_metadata['features'])
    
    # Save data if any was found
        if features_with_metadata['features']:
            filename = save_data(features_with_metadata)
            print(f"\nSuccessfully retrieved {len(features_with_metadata['features'])} lightning records")
            print(f"Data saved to: {filename}")
        else:
            print(f"\nNo lightning data found for {DATASETS[idx]['label'].replace('_',' ')}.")

    # Create ground strokes and daily lightning file 
    
if __name__ == "__main__":
    main()