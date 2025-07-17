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
PLOTVAR_idx = {'PSA': 1, 'Zone': 1} # which column to plot for region by GROUPINGS list index
PLOTSTARTDATE = '2025-05-14'  # start plotting from this date
YEAR = 2025

def download_reports(startdatestr: str, 
                     URLtemplate: str, 
                     fntemplate: str,
                     outdir: Path, 
                     overwrite: bool=False) -> str:
    """Download situation reports from AICC site"""
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
    """Construct this year's dataframe from downloaded files"""
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
    """Join all updates with PSA boundaries"""
    psa_GDF = get_psaGDF(projdir=projdir)
    all_updates_GDF = gdf_from_df(all_updatesDF)
    joined_GDF = gp.sjoin(all_updates_GDF, psa_GDF, predicate='within', how='inner')
    joined_GDF.drop(columns=['index_right', 'GACC', 'ID'], inplace=True)
    return joined_GDF

def aggregate_by_day_region(updatesDF: pd.DataFrame,
                            region: str) -> pd.DataFrame:
    """Aggregate daily burned area by region. Region is one of GROUPINGS keys."""
    try:
        dailyarea_agg = updatesDF[
            GROUPINGS[region] + ['Acres']].groupby(GROUPINGS[region]).sum().reset_index()
    except KeyError:
        print(f"Grouping by {region} is unknown. Try one of : {', '.join(GROUPINGS.keys())}")
    dailyarea_agg['Acres'] = dailyarea_agg['Acres'].replace(0, pd.NA)
    dailyarea_agg.dropna(inplace=True)
    return dailyarea_agg

def load_old_data(olddatafp: Path) -> pd.DataFrame:
    """Load old data from the file"""
    if not olddatafp.exists():
        raise FileNotFoundError(f"Old data file {olddatafp} does not exist.")
    olddata = pd.read_csv(olddatafp, skiprows=2)
    dropcols = [col for col in olddata.columns if col.startswith('Unname')] 
    olddata.drop(columns=dropcols+['ID'], inplace=True)
    intcols = list(olddata.columns.difference(['TotalAcres', 'ProtectionUnit', 'SitReportDate']))
    for col in intcols:
        if olddata[col].isna().any():
            olddata.drop(columns=[col], inplace=True)
        else:
            olddata[col] = olddata[col].astype(int)
    olddata.rename(columns={'FireSeason': 'Year'}, inplace=True)
    olddata['reportdate'] = pd.to_datetime(olddata[['Year', 'Month', 'Day']])
    olddata.drop(columns=['SitReportDate'], inplace=True)
    olddata.rename({'TotalAcres': 'Acres'}, axis=1, inplace=True)
    return olddata

def olddata_to_daily(olddata: pd.DataFrame) -> pd.DataFrame:
    """Convert old data to daily format"""
    olddata.drop(columns=['Month', 'Day', 'TotalFires', 'ProtectionUnit'], inplace=True)
    olddata = olddata.groupby(['reportdate', 'Year']).sum().reset_index()
    olddata.rename({'TotalAcres': 'Acres'}, axis=1, inplace=True)
    return olddata

def reformat_newdata(dailyareabyZoneDF: pd.DataFrame) -> pd.DataFrame:
    """Reformat new daily data to match old data format"""
    dailyareabyZoneDF['Year'] = dailyareabyZoneDF.reportdate.dt.year
    dailyareabyZoneDF['Month'] = dailyareabyZoneDF.reportdate.dt.month
    dailyareabyZoneDF['Day'] = dailyareabyZoneDF.reportdate.dt.day
    dailyareabyZoneDF.rename(columns={'Protecting Office Label': 'ProtectionUnit'}, inplace=True)
    dailyareabyZoneDF.drop(columns=['Protecting Office'], inplace=True)
    return dailyareabyZoneDF 

def combine_daily_totals(olddata: pd.DataFrame,
                        dailyarea: pd.DataFrame) -> pd.DataFrame:
    """Combine old and new daily totals"""
    combined = pd.concat([olddata, dailyarea], ignore_index=True)
    return combined

def plot_dailyarea_by_region(dailyareaDF: pd.DataFrame,
                             region: str,
                             areathreshold: int=3000,
                             figdir: Path=None,
                             plotday: str=None,
                             savefig=True) -> None:
    """Plot daily burned area by region"""
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import seaborn as sns
    import colorcet as cc
    mpl.rcParams['figure.dpi'] = 150
    sns.set_theme('paper')
    sns.set_style('whitegrid')
    if region not in GROUPINGS.keys():
        raise ValueError(f"Region {region} not recognized. Try one of {', '.join(GROUPINGS.keys())}")
    PLOTVAR = GROUPINGS[region][PLOTVAR_idx[region]]
    if areathreshold:
        big_fires_PSAS = dailyareaDF[dailyareaDF.Acres > areathreshold].sort_values(
            'Acres', ascending=False).drop_duplicates(PLOTVAR)[PLOTVAR].to_list()
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.lineplot(data=dailyareaDF.loc[PLOTSTARTDATE:], x='reportdate', y='Acres', 
                hue=PLOTVAR, ax=ax, palette=sns.color_palette(cc.glasbey_dark))
    plt.title(f"{YEAR} daily area burned by {PLOTVAR} (from AICC Situation Reports)")
    plt.xlabel("Date of situation report")
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    plt.tight_layout()
    if savefig:
        outfp = figdir / f"dailyarea_{region}_{plotday}.png"
        fig.savefig(outfp, bbox_inches='tight')
        print(f"Saved figure to {outfp}")
        plt.close()
    else:
        plt.show()
    return None 

def plot_dailytotals_by_year(dailyareaDF: pd.DataFrame,
                             olddata: bool=True,
                             annualthreshold: int=None,
                             figdir: Path=None,
                             today: str=None,
                             savefig=True) -> None:
    """Plot daily burned area totals by year"""
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    import seaborn as sns
    import colorcet as cc
    mpl.rcParams['figure.dpi'] = 150
    sns.set_theme('paper')
    sns.set_style('whitegrid')
    fig, ax = plt.subplots(figsize=(10, 6))
    if olddata:
        dailyareaDF['reportdate'] = dailyareaDF.reportdate.map(lambda t: t.replace(year=YEAR))
        if annualthreshold:
            big_fires_years = dailyareaDF[dailyareaDF.Acres > annualthreshold].sort_values(
                'Acres', ascending=False).drop_duplicates('Year').Year.to_list()
            dailyareaDF = dailyareaDF[dailyareaDF.Year.isin(big_fires_years)]
        sns.lineplot(data=dailyareaDF, x='reportdate', y='Acres', 
                    hue='Year', ax=ax, palette=sns.color_palette(cc.glasbey))
        ax.xaxis.set_major_formatter(mpl.dates.DateFormatter('%m-%d'))
        plt.title("Daily area burned by Fire Season")
    else:
        sns.lineplot(data=dailyareaDF.loc[PLOTSTARTDATE:], x='reportdate', y='Acres', 
                    ax=ax)
        plt.title(f"{YEAR} daily area burned (from AICC Situation Reports)")
    ax.yaxis.set_major_formatter(mpl.ticker.StrMethodFormatter('{x:,.0f}'))
    ax.set_xlabel("Date")
    plt.tight_layout()
    if savefig:
        outfp = figdir / f"dailytotals_by_year_{'olddata' if olddata else '2025only'}_{today}.png"
        fig.savefig(outfp, bbox_inches='tight')
        print(f"Saved figure to {outfp}")
        plt.close()
    else:
        plt.show()
    return None