# Tools for retrieving and aggregating AK burned area daily tally
# from situation reports at AICC. 

import pandas as pd
import geopandas as gp
import requests
import datetime as dt
from pathlib import Path

PSA_relative_path = "resources/AK_predictive_service_areas.json"
PROTECTING_OFFICES = {'MSS': 'Mat-Su Area',
        'TNF': 'Tongass N.F.',
        'UYD': 'Upper Yukon Zone',
        'KKS': 'Kenai-Kodiak Area',
        'CRS': 'Copper River Area',
        'TAD': 'Tanana Zone',
        'DAS': 'Delta Area',
        'FAS': 'Fairbanks Area',
        'MID': 'Military Zone',
        'CGF': 'Chugach N.F.',
        'TAS': 'Tok Area',
        'GAD': 'Galena Zone',
        'SWS': 'Southwest Area'}
GROUPINGS = {
        'PSA': ['reportdate', 'PSA_NAME', 'NAT_CODE'],
        'Zone': ['reportdate', 'Protecting Office', 'Protecting Office Label']
}

def download_reports(startdatestr: str, 
                     URLtemplate: str, 
                     fntemplate: str,
                     outdir: Path, 
                     overwrite: bool=False) -> str:
    """Download situationrreports from AICC site"""
    lastdate = None
    for item in pd.date_range(startdatestr,dt.datetime.now().strftime("%Y%m%d"), freq='d'):
        thedate = item.strftime('%Y%m%d')
        outpath = outdir / f"{fntemplate}{thedate}.xlsx"
        if outpath.exists() and not overwrite:
            print(f"File {outpath} already exists. Skipping download.")
            continue
        URL = URL = f"{URLtemplate}{item.strftime('%m_%Y')}/{fntemplate}{thedate}.xlsx"
        response = requests.get(URL)
        if response.status_code == 200:
            with open(outpath, 'wb') as dst:
                dst.write(response.content)
            print(f"Downloaded {URL}")
            lastdate = thedate
        else:
            URL = f"{URLtemplate}{fntemplate}{thedate}.xlsx"
            response = requests.get(URL)
            if response.status_code == 200:
                with open(outpath, 'wb') as dst:
                    dst.write(response.content)
                print(f"Downloaded {URL}")
                lastdate = thedate
            else:
                print(f"File not present on server: {URL}")
    return lastdate

def extract_zone(row: pd.Series) -> str:
    """Extract the short version of the Protecting Office / FM Zone"""
    try:
        idx = row['Protecting Office'].find('(')
        return PROTECTING_OFFICES[row['Protecting Office'][idx+1:idx+4]]
    except KeyError:
        try:
            return PROTECTING_OFFICES[row['Protecting Office']]
        except KeyError:
            return row['Protecting Office']
        
def assemble_dataframe(datadir:Path, 
                       fntemplate: str) -> pd.DataFrame:
    results = []
    for fp in datadir.glob(f'{fntemplate}*'):
        datestamp = fp.stem[-8:]
        current = pd.read_excel(fp)
        current['reportdate'] = datestamp
        current.drop(columns=['OBJECTID'], inplace=True)
        results.append(current)
    all_updates = pd.concat(results).sort_values(['reportdate'])
    all_updates['reportdate'] = pd.to_datetime(all_updates['reportdate'])
    all_updates['Protecting Office'] = all_updates['Protecting Office'].fillna("n/a")
    all_updates['Protecting Office'] = all_updates.apply(extract_zone, axis=1)
    protecting_offices_rev = {
        PROTECTING_OFFICES[key]: key for key in PROTECTING_OFFICES
    }
    protecting_offices_rev['n/a'] = 'n/a'
    all_updates['Protecting Office Label'] = all_updates['Protecting Office'].map(protecting_offices_rev)
    return all_updates

def get_psaGDF(projdir: Path) -> gp.GeoDataFrame:
    """Load PSA boundaries as GeoDataframe"""
    psafp = projdir / PSA_relative_path
    return gp.read_file(psafp)

def gdf_from_df(updatesDF: pd.DataFrame) -> gp.GeoDataFrame:
    """Turn pandas DataFrame into GeoDataframe"""
    geometry = gp.points_from_xy(updatesDF['Longitude'], updatesDF['Latitude'])
    return gp.GeoDataFrame(updatesDF, geometry=geometry, crs="EPSG:4326")

def add_psa(all_updatesDF: pd.DataFrame, 
            projdir: Path) -> gp.GeoDataFrame:
    psa_GDF = get_psaGDF(projdir=projdir)
    all_updates_GDF = gdf_from_df(all_updatesDF)
    joined_GDF = gp.sjoin(all_updates_GDF, psa_GDF, predicate='within', how='inner')
    joined_GDF.drop(columns=['index_right', 'GACC', 'ID'], inplace=True)
    return joined_GDF

def aggregate_by_day_region(updatesDF: pd.DataFrame,
                            region: str) -> pd.DataFrame:
    try:
        dailyarea_agg = updatesDF[
            GROUPINGS[region] + ['Acres']].groupby(GROUPINGS[region]).sum().reset_index()
    except KeyError:
        print(f"Grouping by {region} is unknown. Try one of : {', '.join(GROUPINGS.keys())}")
    dailyarea_agg['Acres'] = dailyarea_agg['Acres'].replace(0, pd.NA)
    dailyarea_agg.dropna(inplace=True)
    return dailyarea_agg