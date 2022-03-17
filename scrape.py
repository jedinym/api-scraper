#!/usr/bin/python

import requests as rq
import argparse as ap

from sys import stderr
from os import remove, mkdir, path

from datetime import datetime

import gzip

BASE_URL = "https://api.cryptochassis.com/v1"


def get_end_time(start_time: str, exchange: str, instrument: str) -> str:
    """
    Get end_time from response
    """
    response = rq.get(
        f"{BASE_URL}/trade/{exchange}/{instrument}?startTime={start_time}"
    )

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    data = response.json()
    return data["urls"][0]["endTime"]["seconds"]


def get_file_url(start_time: str, exchange: str, instrument: str) -> str:
    """
    Get url of gzipped file
    """
    response = rq.get(
        f"{BASE_URL}/market-depth/{exchange}/{instrument}?startTime={start_time}"
    )

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    data = response.json()
    return data["urls"][0]["url"]


def download_gzip(url: str, filename: str) -> None:
    """
    Download gzipped file and save it to disk
    """
    response = rq.get(url)

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    with open(f'{filename}', 'wb') as gzipped:
        gzipped.write(response.content)


def write_to_csv(gzipped_file: str, filename: str):
    """
    Decompress gzipped file and save it to disk
    """
    with open(gzipped_file, 'rb') as gzipped:
        with open(filename, 'wb') as csv:
            content = gzipped.read()
            csv.write(gzip.decompress(content))


def days(start_time: str, end_time: str):
    cur_time = start_time
    while int(cur_time) <= int(end_time):
        yield cur_time
        cur_time = str(int(cur_time) + 86400)  # add day


def setup_parser() -> ap.ArgumentParser:
    parser = ap.ArgumentParser()
    parser.add_argument(
        "--init-time",
        dest="init_time",
        type=str,
        nargs="?",
        default="1609455600",
        help="UNIX timestamp of initial time",
    )
    parser.add_argument(
        "--end-time",
        dest="end_time",
        type=str,
        nargs="?",
        default=None,
        help="UNIX timestamp of end time",
    )
    parser.add_argument("--exchange", "-e", dest="exchange", type=str, required=True)
    parser.add_argument(
        "--instrument", "-i", dest="instrument", type=str, required=True
    )
    parser.add_argument("--dest", dest="directory", type=str, required=True, help="RELATIVE path to directory to put csv files into")

    return parser


def get_file_name(start_time: str) -> str:
    day = datetime.utcfromtimestamp(int(start_time)).strftime('%d_%m_%Y')
    return day


def make_dir(dir_name: str):
    if path.exists(dir_name) and path.isdir(dir_name):
        return

    mkdir(dir_name)


if __name__ == "__main__":
    parser = setup_parser()
    args = parser.parse_args()

    end_time = args.end_time
    if end_time is None:
        end_time = get_end_time(args.init_time, args.exchange, args.instrument)

    # makes directory if it doesn't exist
    make_dir(args.directory)

    for start_time in days(args.init_time, end_time):
        url = get_file_url(start_time, args.exchange, args.instrument)

        file_name_date = get_file_name(start_time)
        gzipped_file_name = f'{args.directory}/{file_name_date}.gz'
        csv_file_name = f'{args.directory}/{file_name_date}.csv'

        download_gzip(url, gzipped_file_name)
        write_to_csv(gzipped_file_name, csv_file_name)
        remove(gzipped_file_name)
