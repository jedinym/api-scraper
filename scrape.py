#!/usr/bin/python

import json
from typing import Generator
import requests as rq
import argparse as ap

from sys import stderr

BASE_URL = 'https://api.cryptochassis.com/v1'


def get_end_time(start_time: str, exchange: str, instrument: str) -> str:
    response = rq.get(f'{BASE_URL}/trade/{exchange}/{instrument}?startTime={start_time}')

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    data = response.json()
    return data['urls'][0]['endTime']['seconds']


def get_file_url(start_time: str, exchange: str, instrument: str) -> str:
    response = rq.get(f'{BASE_URL}/market-depth/{exchange}/{instrument}?startTime={start_time}')

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    data = response.json()
    return data['urls'][0]['url']


def get_file(url: str, filename: str) -> bytes:
    response = rq.get(url)

    if response.status_code != 200:
        print(response.content, file=stderr)
        exit(1)

    return response.content


def days(start_time: str, end_time: str):
    cur_time = start_time
    while int(cur_time) <= int(end_time):
        yield cur_time
        cur_time = str(int(cur_time) + 86400)


def setup_parser() -> ap.ArgumentParser:
    parser = ap.ArgumentParser()
    parser.add_argument('--init-time', dest='init_time', type=str, nargs='?', default='1609455600')
    parser.add_argument('--end-time', dest='end_time', type=str, nargs='?', default=None)
    parser.add_argument('--exchange', '-e', dest='exchange', type=str, required=True)
    parser.add_argument('--instrument', '-i', dest='instrument', type=str, required=True)

    return parser


if __name__ == '__main__':
    parser = setup_parser()
    args = parser.parse_args()

    end_time = args.end_time
    if end_time is None:
        end_time = get_end_time(args.init_time, args.exchange, args.instrument)

    for start_time in days(args.init_time, end_time):
        print(start_time)

    # bts = get_file('https://marketdata-e0323a9039add2978bf5b49550572c7c.s3.amazonaws.com/v2/market_depth/cb/btc-usd/1-1647302400.csv.gz?AWSAccessKeyId=ASIATPNB7YZIYD7X3Z7K&Expires=1647446521&Signature=bFDoDR8V3uATmacd7Yp607iHBdM%3D&x-amz-security-token=IQoJb3JpZ2luX2VjEBIaCXVzLWVhc3QtMSJGMEQCIAkF1sQCYWgS7kT7Z7q9ulWnjj8WZv7u4Lo%2F%2FRxF7BovAiBTprNCgufIoUuVakYOTlIFBYLMAmorAFCfzm95vldhSSqDBAiL%2F%2F%2F%2F%2F%2F%2F%2F%2F%2F8BEAIaDDIzOTI0NzI3OTY5NyIMZ435rmKedK9H7tGhKtcDmbZL%2FolT%2BtspSX5JOC0dDJRL9%2FU92s4YGn23GtjzjF%2FScCgAUU7KHLjjTH2wEhleJGNQnA%2BZcLaUxPJenG59pAO7GcEUD2nuOYTDjtZwmUg72%2BOz6xhxcOf5S5yEaxOhvBW5bp6tG6avJUYD1raI%2BoZRduXLXdgSdiADUvPsuxwsOrrTnoiAE3PS6ZMZFaEtzEdkNLfgL9oqXCSnrN%2BxPqKoNxyQI1G0cGQbdAb%2FoYYS3H6pEYwpJj6Qvuvlls1YvLNR7PsEw2TYMVfM00axYDIGGnFeD1RXniZvMzwNsmsG0ypi9SsNbNxEzCCgOPDU6BGbGjuQImkxtaE%2BkDgtOYdMSzMfRUWVOPtw7LxuOIbpT%2BFv9ymQyO76c7zjxpy3%2FvXxysT%2BIAt02rx4i4W7vfhh5WKI7dbxorakj%2BtBOEobrWskecYVyh1ul0%2FbGexMxjWiXlimS8xmunVx4PQGClvSlCF9sZFcB5uDKe7OWHlJwNHhHg6mmw8ZTPsb40TIFZX8DxlfH6W0MlIbNIiP6pVtLCNdFUqCKxx2Z8oiIfgwOQVMzh2z9y8kxyANwn%2FxlEaZ4ivpXKtGR6zbgH810612QZnfY8YpUwNk5TNt33Qyys2Q9zknMJHsxpEGOqYB3iOTjMQT1N4GE449Uc81wyON4L%2FYfk0TCJFI5AzA%2BR9c9UlVg5s0m%2Fz04ev6uOtVHlV50epqtJPU1uRrodq92cZkRtUVosrsjjUsHDiO1SzSuXkUn%2Bk6y51KCvPFlRk7y45bPnEAKwSvXJ2b7pXnPi1uExbn%2BGnDn7QG731bNFzW2PtJ4RWsqHANFoFdrWNHNQu8epq%2Bg926f9osvLic0vJCj0Tl3Q%3D%3D', 'test')
    # with open('a', 'wb') as f:
    #     f.write(bts)
