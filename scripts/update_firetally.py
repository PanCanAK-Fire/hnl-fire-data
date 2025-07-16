# Download and update daily fire tally starting in 2025
# Data comes from AICC situation reports and is aggregated 
# by protecting office (Fire Management Zone) 
# and Predictive Service Area (PSA, for 2025 only)
#
# Chris Waigl, cwaigl@alaska.edu, 2024-07-11

from hnl_fire_data import firetallytools as ft
from pathlib import Path
import datetime as dt
import argparse

# edit these as required

URLTEMPLATE = "https://fire.ak.blm.gov/content/aicc/Previous%20Situation%20Reports/2025%20Situtation%20Report%20Exports/"
FNTEMPLATE = 'AK_SituationReportExport_'
PROJDIR = Path().absolute().parent
FIGDIR = PROJDIR / "figures"
OUTDIR = PROJDIR / "data/working"
REPORTDIR = Path().absolute().parents[1]  / 'data/AICC_reports'   # outside the repo directory - change if desired
OVERWRITE = False       # overwrite files when downloading
STARTDATE = '20250415'  # date to start looking for situation reports
YEAR = 2025
TODAY = dt.datetime.now().strftime("%Y%m%d")

def parse_arguments():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description='Read command line arguments')
    parser.add_argument('-g', '--github_action', 
        help="run as github action",
        action="store_true")

def main() -> None:
    args = parse_arguments()
    if args.github_action: 
        raise Exception("Not implemented yet")
    
    # step 1: Download new reports
    REPORTDIR.mkdir(parents=True, exist_ok=True)
    _ = ft.download_reports(startdatestr=STARTDATE,
                               URLtemplate=URLTEMPLATE,
                               fntemplate=FNTEMPLATE,
                               outdir=REPORTDIR,
                               overwrite=False)
    
    # step 2: assemble the dataframe from all reports and save it
    all_updates = ft.assemble_dataframe(REPORTDIR, FNTEMPLATE)
    all_updates.to_csv(OUTDIR / f"all_updates_{YEAR}_{TODAY}.csv")
    # step 3: clean up, remove Rx and FA fires, add PSAs, save again
    all_updates = all_updates.query("`Incident Type` != 'RX-Prescribed Fire'")
    all_updates = all_updates.query("`Incident Type` != 'FA-False Alarm'")
    all_updates_PSA = ft.add_psa(all_updates, projdir=PROJDIR)
    all_updates_PSA.to_csv(OUTDIR / f"all_updates_PSA_{YEAR}_{TODAY}.csv")
    # step 4: make daily tallies this year by PSA, by Zone, and by year


if __name__ == '__main__':
    main()