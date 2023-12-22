import argparse
from sqlite3 import Error as SqliteError

from pydantic import ValidationError

from ufcstats_scraper.common import console
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.events_list import scrape_events_list
from ufcstats_scraper.scrapers.exceptions import ScraperError
from ufcstats_scraper.scrapers.fighters_list import scrape_fighters_list

# Main argument parser
main_parser = argparse.ArgumentParser(
    prog="ufcstats-scraper",
    description="Program for scraping data from ufcstats.com",
)
main_parser.add_argument(
    "-q",
    "--quiet",
    action="store_true",
    dest="quiet",
    help="suppress output",
)
subparsers = main_parser.add_subparsers(title="subcommands")


# events-list subcommand
def events_list(args: argparse.Namespace) -> None:
    console.quiet = args.quiet
    scrape_events_list()


parser_events_list = subparsers.add_parser(
    "events-list",
    description="Subcommand for scraping the events list",
    help="scrape the events list",
)
parser_events_list.set_defaults(func=events_list)


# fighters-list subcommand
def fighters_list(args: argparse.Namespace) -> None:
    console.quiet = args.quiet
    scrape_fighters_list(args.delay)


parser_fighters_list = subparsers.add_parser(
    "fighters-list",
    description="Subcommand for scraping the fighters list",
    help="scrape the fighters list",
)
parser_fighters_list.add_argument(
    "-d",
    "--delay",
    type=float,
    default=DEFAULT_DELAY,
    dest="delay",
    help="set delay between requests",
)
parser_fighters_list.set_defaults(func=fighters_list)

# Parse arguments and run subcommand
args = main_parser.parse_args()
try:
    args.func(args)
except (DBNotSetupError, OSError, ScraperError, SqliteError, ValidationError, ValueError):
    console.quiet = False
    console.print_exception()
    exit(1)
