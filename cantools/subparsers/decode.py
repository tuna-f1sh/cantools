from __future__ import print_function
import sys
import os
import time
import datetime
import re
import binascii
import struct
import uuid

from influxdb import InfluxDBClient

from .. import database
from .utils import format_message_by_frame_id

IMPORT_TIME = datetime.datetime.utcfromtimestamp(time.time()).strftime('%Y-%m-%dT%H:%M:%SZ')
HOSTNAME = os.uname().nodename
UUID = str(uuid.uuid4())

# Matches 'candump' output, i.e. "vcan0  1F0   [8]  00 00 00 00 00 00 1B C1".
RE_CANDUMP = re.compile(r'^.*  ([0-9A-F]+)   \[\d+\]\s*([0-9A-F ]*)$')
# Matches 'candump -L' log file, i.e. "(1575305161.758357) can0 201#0000000062".
RE_CANDUMP_LOG = re.compile(r'^\(([0-9]{10}.[0-9]{6})\) ([A-Za-z]{3,4}[0-9]{1,2}) ([0-9A-Za-z]{3,8})#([0-9A-Za-z]{2,16})$')

def _log_mo_unpack(mo):
    timestamp = mo.group(1)
    # convert to int and to ms by stripping last 3 digits
    timestamp = int(timestamp.replace('.','')[:-3])
    # timestamp = float(timestamp)
    # timestamp = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    link = mo.group(2)
    frame_id = mo.group(3)
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group(4)
    data = data.ljust(16, '0')
    data = binascii.unhexlify(data)

    return timestamp, link, frame_id, data

def _mo_unpack(mo):
    timestamp = time.time()
    # timestamp = datetime.datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    timestamp = int(timestamp / 1000)
    link = mo.group(0)
    frame_id = mo.group(1)
    frame_id = '0' * (8 - len(frame_id)) + frame_id
    frame_id = binascii.unhexlify(frame_id)
    frame_id = struct.unpack('>I', frame_id)[0]
    data = mo.group(2)
    data = data.replace(' ', '')
    data = binascii.unhexlify(data)

    return timestamp, link, frame_id, data

def _write_to_db(db, json, host='127.0.0.1', port=8086):
    client = InfluxDBClient(host=host, port=port, database=db)
    client.create_database(db)
    # if client.write_points(json, time_precision='u', batch_size=100):
    #     print('Write complete')
    # else:
    #     print('Write failed')
    for x in range(0, len(json), 100):
        if client.write_points(json[x:x+100], time_precision='ms'):
            print('Chunk {} of {} write complete'.format(x/100, int(len(json)/100)))
        else:
            print('Write {} of {} failed'.format(x, int(len(json)/100)))


def _do_decode(args):
    dbase = database.load_file(args.database,
                               encoding=args.encoding,
                               frame_id_mask=args.frame_id_mask,
                               strict=not args.no_strict)
    decode_choices = not args.no_decode_choices

    influx_json = []

    try:
        while True:
            line = sys.stdin.readline()

            # Break at EOF.
            if not line:
                if args.influxdb:
                    _write_to_db(args.influxdb, influx_json, args.influxdb_host, args.influxdb_port)
                break

            line = line.strip('\r\n')

            # TODO dict with function calls for type
            if args.filetype == 'dump':
                mo = RE_CANDUMP.match(line)
                if mo: timestamp, link, frame_id, data = _mo_unpack(mo)
                else: break
            elif args.filetype == 'log':
                mo = RE_CANDUMP_LOG.match(line)
                if mo: timestamp, link, frame_id, data = _log_mo_unpack(mo)
                else: break
            else:
                # argparse should not let us get here
                print('Invalid filetype!')
                break


            line += ' ::'
            line += format_message_by_frame_id(dbase,
                                               frame_id,
                                               data,
                                               decode_choices,
                                               args.single_line)

            if not args.quiet: print(line)

            if args.influxdb:
                signals = dbase.decode_message(frame_id, data, decode_choices=False)
                message = dbase.get_message_by_frame_id(frame_id)

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

                    if not args.quiet: print(json)
                    # _write_to_db(args.influxdb, [json], args.influxdb_host, args.influxdb_port)

    except KeyboardInterrupt:
        if args.influxdb:
            print('Interupted! Please wait whilst writing to InfluxDB')
            _write_to_db(args.influxdb, influx_json, args.influxdb_host, args.influxdb_port)



def add_subparser(subparsers):
    decode_parser = subparsers.add_parser(
        'decode',
        description=('Decode "candump" CAN frames read from standard input '
                     'and print them in a human readable format.'))
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
        '--influxdb-port',
        default='8086',
        help='InfluxDB server port')
    decode_parser.add_argument(
        '-f', '--filetype',
        default='dump',
        choices=['dump', 'log'],
        help='CAN file input type')
    decode_parser.add_argument(
        '-e', '--encoding',
        help='File encoding.')
    decode_parser.add_argument(
        '--no-strict',
        action='store_true',
        help='Skip database consistency checks.')
    decode_parser.add_argument(
        '-m', '--frame-id-mask',
        type=lambda x: int(x, 0),
        help=('Only compare selected frame id bits to find the message in the '
              'database. By default the candump and database frame ids must '
              'be equal for a match.'))
    decode_parser.add_argument(
        'database',
        help='Database file.')
    decode_parser.set_defaults(func=_do_decode)
