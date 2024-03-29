import argparse
import sys
from sqlite3 import Error as SqliteError
from typing import get_args

from pydantic import ValidationError

from ufcstats_scraper import config
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.setup import setup_db
from ufcstats_scraper.scrapers.event_details import scrape_event_details
from ufcstats_scraper.scrapers.events_list import scrape_events_list
from ufcstats_scraper.scrapers.exceptions import ScraperError
from ufcstats_scraper.scrapers.fight_details import scrape_fight_details
from ufcstats_scraper.scrapers.fighter_details import scrape_fighter_details
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
subparsers = main_parser.add_subparsers(title="subcommands", required=True)


# db-setup subcommand
def db_setup(args: argparse.Namespace) -> None:
    console.quiet = args.quiet
    setup_db(reset=args.reset)


parser_db_setup = subparsers.add_parser(
    "db-setup",
    description="Subcommand for setting up the links database",
    help="set up links database",
)
parser_db_setup.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
parser_db_setup.set_defaults(func=db_setup)


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
    default=config.default_delay,
    dest="delay",
    help="set delay between requests",
)
parser_fighters_list.set_defaults(func=fighters_list)

# Parent parser for "details scrapers"
parser_details = argparse.ArgumentParser(add_help=False)
parser_details.add_argument(
    "-d",
    "--delay",
    type=float,
    default=config.default_delay,
    dest="delay",
    help="set delay between requests",
)
parser_details.add_argument(
    "-f",
    "--filter",
    type=str,
    choices=get_args(LinkSelection),
    default=config.default_select,
    dest="select",
    help="filter entries in the database",
)
parser_details.add_argument(
    "-l",
    "--limit",
    type=int,
    default=-1,
    dest="limit",
    help="limit the number of items to scrape",
)


# event-details subcommand
def event_details(args: argparse.Namespace) -> None:
    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    scrape_event_details(args.select, limit, args.delay)


parser_event_details = subparsers.add_parser(
    "event-details",
    parents=[parser_details],
    description="Subcommand for scraping event details",
    help="scrape event details",
)
parser_event_details.set_defaults(func=event_details)


# fighter-details subcommand
def fighter_details(args: argparse.Namespace) -> None:
    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    scrape_fighter_details(args.select, limit, args.delay)


parser_fighter_details = subparsers.add_parser(
    "fighter-details",
    parents=[parser_details],
    description="Subcommand for scraping fighter details",
    help="scrape fighter details",
)
parser_fighter_details.set_defaults(func=fighter_details)


# fight-details subcommand
def fight_details(args: argparse.Namespace) -> None:
    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    scrape_fight_details(args.select, limit, args.delay)


parser_fight_details = subparsers.add_parser(
    "fight-details",
    parents=[parser_details],
    description="Subcommand for scraping fight details",
    help="scrape fight details",
)
parser_fight_details.set_defaults(func=fight_details)

# Parse arguments and run subcommand
args = main_parser.parse_args()
try:
    args.func(args)
except (DBNotSetupError, OSError, ScraperError, SqliteError, ValidationError):
    console.quiet = False
    console.print_exception()
    sys.exit(1)
