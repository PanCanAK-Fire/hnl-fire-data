# Download and update daily fire tally starting in 2025
# Data comes from AICC situation reports and is aggregated 
# by protecting office (Fire Management Zone) 
# and Predictive Service Area (PSA, for 2025 only)
#
# Chris Waigl, cwaigl@alaska.edu, 2024-07-11

from hnl_fire_data import firetallytools as ft
from pathlib import Path
import datetime as dt
import pandas as pd
import argparse

# edit these as required

URLTEMPLATE = "https://fire.ak.blm.gov/content/aicc/Previous%20Situation%20Reports/2025%20Situtation%20Report%20Exports/"
FNTEMPLATE = 'AK_SituationReportExport_'
PROJDIR = Path().absolute().parent
FIGDIR = PROJDIR / "figures"
OUTDIR = PROJDIR / "data/working"
REPORTDIR = Path().absolute().parents[1]  / 'data/AICC_reports'   # outside the repo directory - change if desired
DOWNLOAD = False            # set to False to skip downloading new reports
OVERWRITE = False           # overwrite files when downloading
STARTDATE = '20250420'      # date to start looking for situation reports
TODAY = dt.datetime.now().strftime("%Y%m%d") # reference date in YYYYMMDD format
TODAY = '20250720'
ENDOFYEAR = '20251101'      # date to stop the tally for the year; set to None for all available data
THRESHHOLD_ANNUAL = 600000  # plot only years above this; set to None for all years
THRESHHOLD_REGIONAL = 3000  # plot only PSAs/Zones above this; set to none for all regions
YEAR = 2025
TODAY = dt.datetime.now().strftime("%Y%m%d")
PLOT_OLD_YEARS = True  # if True, plot daily totals for previous years as well
OLDDATAFP = PROJDIR / "resources/AICC_Daily_Stats_2004_to_Present.csv"
EXCLUDE_TYPES = ['RX-Prescribed Fire',      # Incident types we exclude from the tally
                 'FA-False Alarm']

def parse_arguments():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description='Read command line arguments')
    parser.add_argument('-g', '--github_action', 
        help="run as github action",
        action="store_true")
    return parser.parse_args()

def main() -> None:
    args = parse_arguments()
    if args.github_action: 
        raise Exception("Not implemented yet")
    
    # step 1: Download new reports
    if not DOWNLOAD:
        print("DOWNLOAD is set to False, skipping download of reports.")
        print(f"Set DOWNLOAD=True in {__file__} to download new reports.")
        print(f"Using reports in {REPORTDIR}")
    else:
        print("DOWNLOAD is set to True, downloading new reports.")
        print(f"Overwrite existing files is set to {OVERWRITE}")
        print(f"Starting download from {STARTDATE} using URL template {URLTEMPLATE}")
        REPORTDIR.mkdir(parents=True, exist_ok=True)
        print(f"Downloading reports to {REPORTDIR}")
        _ = ft.download_reports(startdatestr=STARTDATE,
                                URLtemplate=URLTEMPLATE,
                                fntemplate=FNTEMPLATE,
                                outdir=REPORTDIR,
                                overwrite=False)
    
    # step 2: assemble the dataframe from all reports and save it
    OUTDIR.mkdir(parents=True, exist_ok=True)
    all_updates = ft.assemble_dataframe(REPORTDIR, FNTEMPLATE)
    all_updates.to_csv(OUTDIR / f"all_updates_{YEAR}_{TODAY}.csv")
    print(f"The all updates dataframe has been assembled with {len(all_updates)} records.")

    # step 3: clean up, remove Rx and FA fires, add PSAs, save again
    all_updates.dropna(subset=['Incident Type'], inplace=True) # remove rows with no incident type (Complexes)
    for it in EXCLUDE_TYPES: 
        all_updates = all_updates.query(f"`Incident Type` != '{it}'")  # remove excluded incident types
    all_updates_PSA = ft.add_psa(all_updates, projdir=PROJDIR)
    all_updates_PSA.to_csv(OUTDIR / f"all_updates_filtered_withPSA_{YEAR}_{TODAY}.csv", float_format='%.1f')
    
    # step 4: make daily tallies this year by PSA, by Zone, and by year
    dailyarea_by_PSA = ft.aggregate_by_day_region(all_updates_PSA, region='PSA')
    dailyarea_by_PSA.to_csv(OUTDIR / f"dailyarea_PSA_{YEAR}_{TODAY}.csv")
    dailyarea_by_Zone = ft.aggregate_by_day_region(all_updates_PSA, region='Zone')
    dailyarea_by_Zone.to_csv(OUTDIR / f"dailyarea_Zone_{YEAR}_{TODAY}.csv", float_format='%.1f')
    dailyarea = dailyarea_by_Zone[['reportdate', 'Acres']].groupby('reportdate').sum()
    dailyarea.to_csv(OUTDIR / f"dailyarea_total_{YEAR}_{TODAY}.csv", float_format='%.1f')

    # step 5: plot daily regional tallies for the current year
    FIGDIR.mkdir(parents=True, exist_ok=True)
    ft.plot_dailyarea_by_region(dailyarea_by_PSA, region='PSA', figdir=FIGDIR,
                            areathreshold=THRESHHOLD_REGIONAL, plotday=TODAY)
    ft.plot_dailyarea_by_region(dailyarea_by_Zone, region='Zone', figdir=FIGDIR, 
                            areathreshold=THRESHHOLD_REGIONAL, plotday=TODAY)

    # step 6: plot daily totals for the current year and if desired, previous years
    if PLOT_OLD_YEARS:
        if OLDDATAFP.exists():
            olddata = ft.load_old_data(OLDDATAFP)
            dailyarea_by_Zone_reformatted = ft.reformat_newdata(dailyarea_by_Zone)
            alldaily_totals = ft.combine_daily_totals(olddata, dailyarea_by_Zone_reformatted)
            alldaily_totals.to_csv(OUTDIR / f"daily_tally_history_by_Zone_to_{TODAY}.csv", 
                            index=False, float_format='%.1f')
            olddata_daily = ft.olddata_to_daily(olddata)
            dailyarea['Year'] = dailyarea.index.year
            dailyarea.reset_index(inplace=True)
            all_data_daily = pd.concat([olddata_daily, dailyarea])
            ft.plot_dailytotals_by_year(all_data_daily, olddata=True, 
                            annualthreshold=THRESHHOLD_ANNUAL, figdir=FIGDIR, today=TODAY)
            return
        else:
            print(f"Old data file {OLDDATAFP} not found. Skipping plotting of previous years.")
    ft.plot_dailytotals_by_year(dailyarea, olddata=False, figdir=FIGDIR, today=TODAY)

if __name__ == '__main__':
    main()