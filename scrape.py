#!/usr/bin/python

from typing import List

import requests as rq
import argparse as ap

from sys import stderr
from os import remove, mkdir, path


from datetime import datetime

import gzip
import csv

import pandas as pd

DELETE_HEADER = False

BASE_URL = "https://api.cryptochassis.com/v1"


def get_end_time(start_time: str, exchange: str, instrument: str) -> str:
    """
    Get end_time from response
    """
    query = f"{BASE_URL}/trade/{exchange}/{instrument}?startTime={start_time}"
    print("GET  " + query)
    response = rq.get(query)

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    data = response.json()
    return data["urls"][0]["endTime"]["seconds"]


def get_file_url(start_time: str, exchange: str, instrument: str) -> str:
    """
    Get url of gzipped file
    """
    query = f"{BASE_URL}/market-depth/{exchange}/{instrument}?startTime={start_time}"
    print("GET  " + query)
    response = rq.get(query)

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

    with open(f"{filename}", "wb") as gzipped:
        gzipped.write(response.content)


def write_to_csv(gzipped_file: str, filename: str):
    """
    Decompress gzipped file and save it to disk
    """
    with open(gzipped_file, "rb") as gzipped:
        with open(filename, "wb") as csv:
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
    parser.add_argument(
        "--dest",
        dest="directory",
        type=str,
        required=True,
        help="RELATIVE path to directory to put csv files into",
    )
    parser.add_argument(
        "--delete-header",
        dest="delete_header",
        action="store_const",
        const=True,
        default=False,
        help="Delete header in final csv file",
    )

    return parser


def get_file_name(start_time: str) -> str:
    day = datetime.utcfromtimestamp(int(start_time)).strftime("%Y_%m_%d")
    return day


def make_dir(dir_name: str):
    if path.exists(dir_name) and path.isdir(dir_name):
        return

    mkdir(dir_name)


def remove_second(in_str: str) -> str:
    return in_str.split("_")[0]


def cull_columns(columns: List[str], csv_to_edit: str, out_csv: str, header: bool) -> None:
    source = pd.read_csv(csv_to_edit)

    for column in columns:
        source = source.drop(column, axis=1)

    first_col = source.iloc[:, [0]]

    source = pd.concat([first_col, source["ask_price_ask_size"].apply(remove_second)], axis=1)

    source.to_csv(out_csv, index=False, header=header)


def download_csv_data(url: str, start_time: str, header: bool, base_dir: str) -> None:
    file_name_date = get_file_name(start_time)
    gzipped_file_name = f"{base_dir}/{file_name_date}.gz"

    no_edit_csv_file_name = f"{base_dir}/{file_name_date}.ne"

    download_gzip(url, gzipped_file_name)
    write_to_csv(gzipped_file_name, no_edit_csv_file_name)
    remove(gzipped_file_name)

    edit_csv_file_name = f"{base_dir}/{file_name_date}.csv"
    cull_columns(["bid_price_bid_size"], no_edit_csv_file_name, edit_csv_file_name, header)
    remove(no_edit_csv_file_name)


def already_present(start_time: str, base_dir: str) -> bool:
    file_name = get_file_name(start_time)
    return path.exists(f'{base_dir}/f{file_name}.csv')


if __name__ == "__main__":
    parser = setup_parser()
    args = parser.parse_args()

    end_time = args.end_time
    if end_time is None:
        end_time = get_end_time(args.init_time, args.exchange, args.instrument)

    # makes directory if it doesn't exist
    make_dir(args.directory)

    for start_time in days(args.init_time, end_time):
        if already_present(start_time, args.directory):
            continue

        url = get_file_url(start_time, args.exchange, args.instrument)

        download_csv_data(url, start_time, not args.delete_header, args.directory)
