from .arable_api import gather_data
import click
import datetime as dt
from pathlib import Path

@click.command()
@click.option('-f', '--folder', default="./",
               help='Folder where to save CSV files')
def main(folder):
    files  = [f for f in  Path(folder).glob("*.csv")]
    if not files: # No files, start downloading yesterday
        gather_data(folder)
    else:
        dates = sorted([dt.datetime.strptime(f.name.split("_")[0], "%Y-%m-%d")
                 for f in files])
        # find the latest date available and start on the following day
        gather_data(folder, start_time=dates[-1] + dt.timedelta(days=1))

main()
