import argparse
import sys
import os
import time
import datetime
import re
import binascii
import struct
import uuid
from argparse_addons import Integer

from tqdm import tqdm
from influxdb import InfluxDBClient

from .. import database
from .. import logreader
from .utils import format_message_by_frame_id

IMPORT_TIME = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
HOSTNAME = os.uname().nodename
UUID = str(uuid.uuid4())

def _human_timestamp(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%fZ')

def _write_to_db(client, json, cs=100):
    for x in tqdm(range(0, len(json), cs), unit='chunk', desc='Posting chunks to InfluxDB server'):
        if not client.write_points(json[x:x+cs], time_precision='n'):
            print('Write {} of {} failed'.format(int(x), int(len(json)/cs)))
    # last bit if wasn't multiple of cs
    if x < len(json):
        client.write_points(json[x-cs::], time_precision='n')


def _do_decode(args):
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               strict=not args.no_strict)
    decode_choices = not args.no_decode_choices

    if (args.influxdb):
        client = InfluxDBClient(host=args.influxdb_host,port=args.influxdb_port, verify_ssl=args.influxdb_ssl, ssl=args.influxdb_ssl, path=args.influxdb_path, database=args.influxdb, username=args.influxdb_username, password=args.influxdb_password)
        client.create_database(args.influxdb)
        influx_json = []
        writing = False

    try:
        if args.input:
            fh = open(args.input, 'r')
        else:
            fh = sys.stdin

        parser = logreader.Parser(fh)
        for line, frame in parser.iterlines(keep_unknowns=True):
            if frame is not None:
                if not frame.frame_id in args.id_filter and len(args.id_filter) > 0: continue

                try:
                    line += ' ::'
                    line += format_message_by_frame_id(dbase,
                                                       frame.frame_id,
                                                       frame.data,
                                                       decode_choices,
                                                       args.single_line)
                except KeyError:
                    print('Skipping as not in DB')
                    continue

                if not args.quiet: print(line)

                try:
                    if args.influxdb:
                        signals = dbase.decode_message(frame.frame_id, frame.data, decode_choices=False)
                        message = dbase.get_message_by_frame_id(frame.frame_id)
                        timestamp = int(frame.timestamp.timestamp() * 1e9)

                        if not message in args.message_filter and len(args.message_filter) > 0: continue

                        if message and signals:
                            json = {
                                "measurement": message.name,
                                "tags": {
                                    "link": link,
                                    "import_time": IMPORT_TIME,
                                    "host": HOSTNAME,
                                    "uuid": UUID,
                                    "dbc": args.database
                                },
                                "time": timestamp,
                                "fields": signals
                            }
                            influx_json.append(json)

                            if not args.quiet: print('InfluxDB json: {}'.format(json))
                except KeyError:
                    print('Skipping write')

        if args.influxdb:
            writing = True
            if args.filetype == 'log':
                print('Saving historical ride {} -> {}. UUID: {}'.format(_human_timestamp(influx_json[0]['time']), _human_timestamp(influx_json[-1]['time']), UUID))
            _write_to_db(client, influx_json)


    except KeyboardInterrupt:
        if args.influxdb and not writing:
            print('Interupted! Please wait whilst writing to InfluxDB')
            _write_to_db(client, influx_json)



def add_subparser(subparsers):
    decode_parser = subparsers.add_parser(
        'decode',
        description=('Decode "candump" CAN frames read from standard input '
                     'and print them in a human readable format.'),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    decode_parser.add_argument(
        '-c', '--no-decode-choices',
        action='store_true',
        help='Do not convert scaled values to choice strings.')
    decode_parser.add_argument(
        '-s', '--single-line',
        action='store_true',
        help='Print the decoded message on a single line.')
    decode_parser.add_argument(
        '-q', '--quiet',
        action='store_true',
        help='Do not print decoding to stdout')
    decode_parser.add_argument(
        '-i', '--influxdb',
        help='Write decoded signal to influxdb database name')
    decode_parser.add_argument(
        '--influxdb-host',
        default='127.0.0.1',
        help='InfluxDB server host address')
    decode_parser.add_argument(
        '--influxdb-ssl',
        action='store_true',
        help='InfluxDB server use ssl')
    decode_parser.add_argument(
        '--influxdb-path',
        default='',
        help='InfluxDB server path when using reverse proxy')
    decode_parser.add_argument(
        '--influxdb-port',
        default='8086',
        help='InfluxDB server port')
    decode_parser.add_argument(
        '--influxdb-username',
        default='root',
        help='InfluxDB server user')
    decode_parser.add_argument(
        '--influxdb-password',
        default='root',
        help='InfluxDB server password')
    decode_parser.add_argument(
        '-f', '--filetype',
        default='dump',
        choices=['dump', 'log'],
        help='CAN file input type')
    decode_parser.add_argument(
        '-I', '--input',
        help='Input file rather than stdin')
    decode_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    decode_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    decode_parser.add_argument(
        '-if', '--id_filter',
        nargs='+',
        default=[],
        type=lambda x: int(x, 0),
        help='Filter hex IDs')
    decode_parser.add_argument(
        '-mf', '--message_filter',
        nargs='+',
        default=[],
        type=str,
        help='Filter messages')
    decode_parser.add_argument(
        '-m', '--frame-id-mask',
        type=Integer(0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the candump and database frame ids must '
              'be equal for a match.'))
    decode_parser.add_argument(
        'database',
        help='Database file.')
    decode_parser.set_defaults(func=_do_decode)
