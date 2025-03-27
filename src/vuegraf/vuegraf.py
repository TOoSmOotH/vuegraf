#!/usr/bin/env python3

__author__ = 'https://github.com/jertel'
__license__ = 'MIT'
__contributors__ = 'https://github.com/jertel/vuegraf/graphs/contributors'
__version__ = '1.8.0'
__versiondate__ = '2025/02/01'
__maintainer__ = 'https://github.com/jertel'
__github__ = 'https://github.com/jertel/vuegraf'
__status__ = 'Production'

import datetime
import json
import sys
import time
import traceback
from threading import Event
import argparse
import pytz

# InfluxDB v1
import influxdb

# InfluxDB v2
import influxdb_client

from pyemvue import PyEmVue
from pyemvue.enums import Scale, Unit

# Import local modules
import src.vuegraf.log as logger
import src.vuegraf.device as device
import src.vuegraf.datapoint as datapoint
import src.vuegraf.config as config

def main():
    """Main entry point for Vuegraf."""
    startup_time = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
    try:
        # Parse command line arguments
        parser = argparse.ArgumentParser(
            prog='vuegraf.py',
            description='Veugraf retrieves energy usage data from commerical cloud servers and inserts it into a self-hosted InfluxDB database.',
            epilog='For more information visit: ' + __github__
        )
        parser.add_argument(
            'configFilename',
            help='JSON config file',
            type=str
        )
        parser.add_argument(
            '-v',
            '--verbose',
            help='Verbose output - shows additional collection information',
            action='store_true'
        )
        parser.add_argument(
            '-d',
            '--debug',
            help='Debug output - shows all point data being collected and written to the DB (can be thousands of lines of output)',
            action='store_true'
        )
        parser.add_argument(
            '--historydays',
            help='Starts execution by pulling history of Hours and Day data for specified number of days.  example: --historydays 60',
            type=int,
            default=0
        )
        parser.add_argument(
            '--resetdatabase',
            action='store_true',
            default=False,
            help='Drop database and create a new one. USE WITH CAUTION - WILL RESULT IN COMPLETE VUEGRAF DATA LOSS!'
        )
        parser.add_argument(
            '--dryrun',
            help='Read from the API and process the data, but do not write to influxdb',
            action='store_true',
            default=False
        )
        args = parser.parse_args()
        logger.info('Starting Vuegraf version {}'.format(__version__))

        # Load configuration
        cfg = {}
        with open(args.configFilename) as config_file:
            cfg = json.load(config_file)

        # Set up InfluxDB
        influx_version = 1
        if 'version' in cfg['influxDb']:
            influx_version = cfg['influxDb']['version']

        bucket = ''
        write_api = None
        query_api = None
        ssl_verify = True

        if 'ssl_verify' in cfg['influxDb']:
            ssl_verify = cfg['influxDb']['ssl_verify']

        if influx_version == 2:
            logger.info('Using InfluxDB version 2')
            bucket = cfg['influxDb']['bucket']
            org = cfg['influxDb']['org']
            token = cfg['influxDb']['token']
            url = cfg['influxDb']['url']
            influx2 = influxdb_client.InfluxDBClient(
                url=url,
                token=token,
                org=org,
                verify_ssl=ssl_verify
            )
            write_api = influx2.write_api(write_options=influxdb_client.client.write_api.SYNCHRONOUS)
            query_api = influx2.query_api()

            if args.resetdatabase:
                logger.info('Resetting database')
                delete_api = influx2.delete_api()
                start = '1970-01-01T00:00:00Z'
                stop = startup_time.isoformat(timespec='seconds').replace("+00:00", "") + 'Z'
                delete_api.delete(start, stop, '_measurement="energy_usage"', bucket=bucket, org=org)
        else:
            logger.info('Using InfluxDB version 1')

            ssl_enable = False
            if 'ssl_enable' in cfg['influxDb']:
                ssl_enable = cfg['influxDb']['ssl_enable']

            # Only authenticate to ingress if 'user' entry was provided in config
            if 'user' in cfg['influxDb']:
                influx = influxdb.InfluxDBClient(
                    host=cfg['influxDb']['host'],
                    port=cfg['influxDb']['port'],
                    username=cfg['influxDb']['user'],
                    password=cfg['influxDb']['pass'],
                    database=cfg['influxDb']['database'],
                    ssl=ssl_enable,
                    verify_ssl=ssl_verify
                )
            else:
                influx = influxdb.InfluxDBClient(
                    host=cfg['influxDb']['host'],
                    port=cfg['influxDb']['port'],
                    database=cfg['influxDb']['database'],
                    ssl=ssl_enable,
                    verify_ssl=ssl_verify
                )

            influx.create_database(cfg['influxDb']['database'])

            if args.resetdatabase:
                logger.info('Resetting database')
                influx.delete_series(measurement='energy_usage')

        # Get Influx Tag information
        tag_name = 'detailed'
        if 'tagName' in cfg['influxDb']:
            tag_name = cfg['influxDb']['tagName']
        tag_value_second = 'True'
        if 'tagValue_second' in cfg['influxDb']:
            tag_value_second = cfg['influxDb']['tagValue_second']
        tag_value_minute = 'False'
        if 'tagValue_minute' in cfg['influxDb']:
            tag_value_minute = cfg['influxDb']['tagValue_minute']
        tag_value_hour = 'Hour'
        if 'tagValue_hour' in cfg['influxDb']:
            tag_value_hour = cfg['influxDb']['tagValue_hour']
        tag_value_day = 'Day'
        if 'tagValue_day' in cfg['influxDb']:
            tag_value_day = cfg['influxDb']['tagValue_day']
            
        add_station_field = config.get_config_value(cfg, 'addStationField', False)
        max_history_days = config.get_config_value(cfg, 'maxHistoryDays', 720)
        history_days = min(args.historydays, max_history_days)
        history = history_days > 0
        
        # Set up global variables for signal handling
        config.running = True
        config.pause_event = Event()
        config.setup_signal_handlers()
        
        interval_secs = config.get_config_value(cfg, 'updateIntervalSecs', 60)
        detailed_interval_secs = config.get_config_value(cfg, 'detailedIntervalSecs', 3600)
        detailed_data_enabled = config.get_config_value(cfg, 'detailedDataEnabled', False)
        detailed_seconds_enabled = detailed_data_enabled and config.get_config_value(cfg, 'detailedDataSecondsEnabled', True)
        detailed_hours_enabled = detailed_data_enabled and config.get_config_value(cfg, 'detailedDataHoursEnabled', True)
        
        logger.info('Settings -> updateIntervalSecs: {}, detailedDataEnabled: {}, detailedIntervalSecs: {}, detailedDataHoursEnabled: {}, detailedDataSecondsEnabled: {}'.format(
            interval_secs, detailed_data_enabled, detailed_interval_secs, detailed_hours_enabled, detailed_seconds_enabled
        ))
        logger.info('Settings -> historyDays: {}, maxHistoryDays: {}'.format(history_days, max_history_days))    
        
        lag_secs = config.get_config_value(cfg, 'lagSecs', 5)
        account_time_zone_name = config.get_config_value(cfg, 'timezone', None)
        account_time_zone = pytz.timezone(account_time_zone_name) if account_time_zone_name is not None and account_time_zone_name.upper() != "TZ" else None
        logger.info('Settings -> timezone: {}'.format(account_time_zone))
        
        detailed_start_time = startup_time
        past_day = datetime.datetime.now(account_time_zone)
        past_day = past_day.replace(hour=23, minute=59, second=59, microsecond=0)
        history_run = history

        while config.running:
            usage_data_points = []
            now = datetime.datetime.now(datetime.UTC).replace(microsecond=0)
            cur_day = datetime.datetime.now(account_time_zone)
            stop_time = now - datetime.timedelta(seconds=lag_secs)
            seconds_since_last_detail_collection = (stop_time - detailed_start_time).total_seconds()
            collect_details = detailed_data_enabled and detailed_interval_secs > 0 and seconds_since_last_detail_collection >= detailed_interval_secs
            logger.verbose('Starting next event collection; collectDetails={}; secondsSinceLastDetailCollection={}; detailedIntervalSecs={}'.format(
                collect_details, seconds_since_last_detail_collection, detailed_interval_secs
            ), args.verbose)

            for account in cfg['accounts']:
                if 'vue' not in account:
                    account['vue'] = PyEmVue()
                    account['vue'].login(username=account['email'], password=account['password'])
                    logger.info('Login completed')
                    device.populate_devices(account)

                try:
                    device_gids = list(account['deviceIdMap'].keys())
                    usages = account['vue'].get_device_list_usage(device_gids, stop_time, scale=Scale.MINUTE.value, unit=Unit.KWH.value)
                    if usages is not None:
                        for gid, dev in usages.items():
                            datapoint.extract_data_points(
                                account, dev, usage_data_points, stop_time, detailed_start_time,
                                collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                                influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                                add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone
                            )

                    if collect_details and detailed_hours_enabled:
                        past_hour = stop_time - datetime.timedelta(hours=1)
                        past_hour = past_hour.replace(minute=00, second=00, microsecond=0)
                        logger.verbose('Collecting previous hour: {} '.format(past_hour), args.verbose)
                        history_start_time = past_hour
                        usages = account['vue'].get_device_list_usage(device_gids, past_hour, scale=Scale.HOUR.value, unit=Unit.KWH.value)
                        if usages is not None:
                            for gid, dev in usages.items():
                                datapoint.extract_data_points(
                                    account, dev, usage_data_points, stop_time, detailed_start_time,
                                    collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                                    influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                                    add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone,
                                    tag_value_hour, history_start_time
                                )

                    if past_day.day != cur_day.day:
                        usages = account['vue'].get_device_list_usage(device_gids, past_day, scale=Scale.DAY.value, unit=Unit.KWH.value)
                        history_start_time = past_day.astimezone(pytz.UTC)
                        logger.verbose('Collecting previous day: {}Local - {}UTC,  '.format(past_day, history_start_time), args.verbose)
                        if usages is not None:
                            for gid, dev in usages.items():
                                datapoint.extract_data_points(
                                    account, dev, usage_data_points, stop_time, detailed_start_time,
                                    collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                                    influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                                    add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone,
                                    tag_value_day, history_start_time
                                )
                        past_day = datetime.datetime.now(account_time_zone)
                        past_day = past_day.replace(hour=23, minute=59, second=59, microsecond=0)

                    if history:
                        stop_time_history = stop_time.astimezone(account_time_zone)
                        logger.info('Loading historical data: {} day(s) ago'.format(history_days))
                        history_start_time = stop_time_history - datetime.timedelta(history_days)
                        history_start_time = history_start_time.replace(hour=00, minute=00, second=00, microsecond=000000)
                        while history_start_time <= stop_time_history:
                            history_end_time = min(history_start_time + datetime.timedelta(20), stop_time_history)
                            history_end_time = history_end_time.replace(hour=23, minute=59, second=59, microsecond=0)
                            logger.verbose('    {}  -  {}'.format(history_start_time, history_end_time), args.verbose)
                            for gid, dev in usages.items():
                                datapoint.extract_data_points(
                                    account, dev, usage_data_points, stop_time, detailed_start_time,
                                    collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                                    influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                                    add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone,
                                    'History', history_start_time, history_end_time
                                )
                            if not config.running:
                                break
                            history_start_time = history_end_time + datetime.timedelta(1)
                            history_start_time = history_start_time.replace(hour=00, minute=00, second=00, microsecond=000000)
                            # Write to database after each historical batch to prevent timeout issues on large history intervals.
                            logger.info('Submitting datapoints to database; account="{}"; points={}'.format(account['name'], len(usage_data_points)))
                            datapoint.dump_points("Sending to database", usage_data_points, influx_version, args.debug)
                            if args.dryrun:
                                logger.info('Dryrun mode enabled.  Skipping database write.')
                            else:
                                if influx_version == 2:
                                    write_api.write(bucket=bucket, record=usage_data_points)
                                else:
                                    influx.write_points(usage_data_points, batch_size=5000)
                            usage_data_points = []
                            config.pause_event.wait(5)
                        history = False

                    if not config.running:
                        break

                    if not history_run:
                        logger.info('Submitting datapoints to database; account="{}"; points={}'.format(account['name'], len(usage_data_points)))
                        datapoint.dump_points("Sending to database", usage_data_points, influx_version, args.debug)
                        if args.dryrun:
                            logger.info('Dryrun mode enabled.  Skipping database write.')
                        else:
                            if influx_version == 2:
                                write_api.write(bucket=bucket, record=usage_data_points)
                            else:
                                influx.write_points(usage_data_points, batch_size=5000)

                    # Resuming logging of normal datapoints after history collection has completed.
                    if not history and history_run:
                        history_run = False

                except Exception:
                    logger.error('Failed to record new usage data: {}'.format(sys.exc_info()))
                    traceback.print_exc()

            if collect_details:
                detailed_start_time = stop_time + datetime.timedelta(seconds=1)
            config.pause_event.wait(interval_secs)

        logger.info('Finished')
    except SystemExit as e:
        # If sys.exit was 2, then normal syntax exit from help or bad command line, no error message
        if e.code == 0 or e.code == 2:
            sys.exit(0)
        else:
            logger.error('Fatal error: {}'.format(sys.exc_info()))
            traceback.print_exc()

if __name__ == "__main__":
    main()
