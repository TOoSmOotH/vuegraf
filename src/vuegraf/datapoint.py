"""
Data point creation and manipulation functions for Vuegraf.
"""
import datetime
import pprint
import pytz
from pyemvue.enums import Scale, Unit
import src.vuegraf.log as logger
import src.vuegraf.device as device

def create_data_point(account_name, device_name, chan_name, watts, timestamp, detailed, influx_version, tag_name, add_station_field):
    """
    Create a data point for InfluxDB.
    
    Args:
        account_name: The account name.
        device_name: The device name.
        chan_name: The channel name.
        watts: The wattage value.
        timestamp: The timestamp.
        detailed: The detailed tag value.
        influx_version: The InfluxDB version (1 or 2).
        tag_name: The tag name.
        add_station_field: Whether to add the station field.
        
    Returns:
        The data point object.
    """
    data_point = None
    if influx_version == 2:
        import influxdb_client
        data_point = influxdb_client.Point('energy_usage') \
            .tag('account_name', account_name) \
            .tag('device_name', chan_name) \
            .tag(tag_name, detailed) \
            .field('usage', watts) \
            .time(time=timestamp)
        if add_station_field:
            data_point.tag('station_name', device_name)
    else:
        data_point = {
            'measurement': 'energy_usage',
            'tags': {
                'account_name': account_name,
                'device_name': chan_name,
                tag_name: detailed,
            },
            'fields': {
                'usage': watts,
            },
            'time': timestamp
        }
        if add_station_field:
            data_point['tags']['station_name'] = device_name

    return data_point

def dump_points(label, usage_data_points, influx_version, is_debug_enabled):
    """
    Dump data points for debugging.
    
    Args:
        label: The label for the dump.
        usage_data_points: The data points to dump.
        influx_version: The InfluxDB version (1 or 2).
        is_debug_enabled: Whether debug mode is enabled.
    """
    if is_debug_enabled:
        logger.debug(label, is_debug_enabled)
        for point in usage_data_points:
            if influx_version == 2:
                logger.debug('  {}'.format(point.to_line_protocol()), is_debug_enabled)
            else:
                logger.debug(f'  {pprint.pformat(point)}', is_debug_enabled)

def get_last_db_timestamp(device_name, chan_name, point_type, foo_start_time, foo_stop_time, foo_history_flag, 
                         influx_version, bucket, query_api, influx, tag_name, add_station_field, 
                         tag_value_minute, tag_value_second, detailed_interval_secs):
    """
    Get the timestamp of the last record in the database.
    
    Args:
        device_name: The device name.
        chan_name: The channel name.
        point_type: The point type.
        foo_start_time: The start time.
        foo_stop_time: The stop time.
        foo_history_flag: The history flag.
        influx_version: The InfluxDB version (1 or 2).
        bucket: The InfluxDB bucket.
        query_api: The InfluxDB query API.
        influx: The InfluxDB client.
        tag_name: The tag name.
        add_station_field: Whether to add the station field.
        tag_value_minute: The minute tag value.
        tag_value_second: The second tag value.
        detailed_interval_secs: The detailed interval in seconds.
        
    Returns:
        A tuple of (start_time, stop_time, history_flag).
    """
    time_str = ''
    # Get timestamp of last record in database
    # Influx v2
    if influx_version == 2:
        station_filter = ""
        if add_station_field: 
            station_filter = '  r.station_name == "' + device_name + '" and '
        time_col = '_time'
        result = query_api.query('from(bucket:"' + bucket + '") ' +
             '|> range(start: -3w) ' +
             '|> filter(fn: (r) => ' +
             '  r._measurement == "energy_usage" and ' +
             '  r.' + tag_name + ' == "' + point_type + '" and ' +
             '  r._field == "usage" and ' + station_filter +
             '  r.device_name == "' + chan_name + '")' +
             '|> last()')

        if len(result) > 0 and len(result[0].records) > 0:
            last_record = result[0].records[0]
            time_str = last_record['_time'].isoformat()
    else: # Influx v1
        station_filter = ""
        if add_station_field: 
            station_filter = 'station_name = \'' + device_name + '\' AND '
        result = influx.query('select last(usage), time from energy_usage where (' + station_filter + 'device_name = \'' + chan_name + '\' AND ' + tag_name + ' = \'' + point_type + '\')')
        if len(result) > 0:
            time_str = next(result.get_points())['time']

    # Depending on version of Influx, the string format for the time is different.  So strip out the variable timezone bits (along with any microsecond values)
    if len(time_str) > 0:
        time_str = time_str[:19]
        # Now make the string timezone aware again by appending a 'Z' at the end
        if not time_str.endswith('Z') and not time_str[len(time_str)-6] in ('+',"-"):
            time_str = time_str + 'Z'
        
        # Convert the time_str into an aware datetime object.
        db_last_record_time = datetime.datetime.strptime(time_str, '%Y-%m-%dT%H:%M:%S%z').replace(tzinfo=datetime.timezone.utc)

        if point_type == tag_value_minute:
            if db_last_record_time < (foo_stop_time - datetime.timedelta(minutes=2,seconds=foo_stop_time.second)):
                foo_history_flag = True
                foo_start_time = db_last_record_time + datetime.timedelta(minutes=1)
                # Can only back a maximum of 7 days for minute data.  So if last record in DB exceeds 7 days, set the startTime to be 7 days ago.
                if int((foo_stop_time - foo_start_time).total_seconds()) > 604800:      # 7 Days
                    foo_start_time = foo_stop_time - datetime.timedelta(minutes=10080)  # 7 Days
                # Can only get a maximum of 12 hours worth of minute data in a single API call.  If more than 12 hours worth is needed, get data in batches; set stopTime to be 12 hours more than the starttime
                if int((foo_stop_time - foo_start_time).total_seconds()) > 43200:       # 12 Hours
                    foo_stop_time = foo_start_time + datetime.timedelta(minutes=720)    # 12 Hours

        if point_type == tag_value_second:
            if db_last_record_time < (foo_start_time - datetime.timedelta(seconds=2)):
                foo_history_flag = True
                foo_start_time = (db_last_record_time + datetime.timedelta(seconds=1)).replace(microsecond=0)
                # Adjust start or stop times if backfill interval exceeds 1 hour
                if (int((foo_stop_time - foo_start_time).total_seconds()) > 3600):
                    # Can never get more than 1 hour of historical second data if detailedIntervalSecs is set to 1 hour or greater.  Set backfill period to be just the past one hour in that case.
                    if (detailed_interval_secs >= 3600):
                        foo_start_time = foo_stop_time - datetime.timedelta(seconds=3600)   # 1 Hour max since detailedIntervalSecs is 1 hour or more
                    else:
                        # Can only backfill a maximum of 3 hours for second data.  So if last record in DB exceeds 3 hours, set the startTime to be 3 hours ago.
                        if int((foo_stop_time - foo_start_time).total_seconds()) > 10800:        # 3 Hours
                            foo_start_time = foo_stop_time - datetime.timedelta(seconds=10800)   # 3 Hours
                        # Can only get a maximum of 1 hour's worth of second data in a single API call.  If more than 1 hour's worth is needed, get data in batches; set stopTime to be 1 hour more than the starttime
                        if int((foo_stop_time - foo_start_time).total_seconds()) > 3600:         # 1 Hour
                            foo_stop_time = foo_start_time + datetime.timedelta(seconds=3600)    # 1 Hour
    else:
        if point_type == tag_value_minute:
            foo_start_time = foo_start_time - datetime.timedelta(days=7)
            foo_stop_time = foo_start_time + datetime.timedelta(hours=12)
            foo_history_flag = True
        elif point_type == tag_value_second:
            foo_start_time = foo_start_time - datetime.timedelta(hours=3)
            foo_stop_time = foo_start_time + datetime.timedelta(hours=1)
            foo_history_flag = True

    return foo_start_time, foo_stop_time, foo_history_flag

def extract_data_points(account, device, usage_data_points, stop_time, detailed_start_time, 
                       collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                       influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                       add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone,
                       point_type=None, history_start_time=None, history_end_time=None):
    """
    Extract data points from a device.
    
    Args:
        account: The account dictionary.
        device: The device object.
        usage_data_points: The list to append data points to.
        stop_time: The stop time.
        detailed_start_time: The detailed start time.
        collect_details: Whether to collect detailed data.
        detailed_seconds_enabled: Whether detailed seconds are enabled.
        detailed_hours_enabled: Whether detailed hours are enabled.
        influx_version: The InfluxDB version (1 or 2).
        tag_name: The tag name.
        tag_value_minute: The minute tag value.
        tag_value_second: The second tag value.
        tag_value_hour: The hour tag value.
        tag_value_day: The day tag value.
        add_station_field: Whether to add the station field.
        bucket: The InfluxDB bucket.
        query_api: The InfluxDB query API.
        influx: The InfluxDB client.
        detailed_interval_secs: The detailed interval in seconds.
        account_time_zone: The account time zone.
        point_type: The point type.
        history_start_time: The history start time.
        history_end_time: The history end time.
    """
    excluded_detail_channel_numbers = ['Balance', 'TotalUsage']
    minutes_in_an_hour = 60
    seconds_in_a_minute = 60
    watts_in_a_kw = 1000
    device_name = device.lookup_device_name(account, device.device_gid)
    account_name = account['name']

    for chan_num, chan in device.channels.items():
        if chan.nested_devices:
            for gid, nested_device in chan.nested_devices.items():
                extract_data_points(account, nested_device, usage_data_points, stop_time, detailed_start_time, 
                                  collect_details, detailed_seconds_enabled, detailed_hours_enabled,
                                  influx_version, tag_name, tag_value_minute, tag_value_second, tag_value_hour, tag_value_day,
                                  add_station_field, bucket, query_api, influx, detailed_interval_secs, account_time_zone,
                                  point_type, history_start_time, history_end_time)

        chan_name = device.lookup_channel_name(account, chan)

        kwh_usage = chan.usage
        if kwh_usage is not None:
            if point_type is None:
                minute_history_start_time, stop_time_min, minute_history_enabled = get_last_db_timestamp(
                    device_name, chan_name, tag_value_minute, stop_time, stop_time, False,
                    influx_version, bucket, query_api, influx, tag_name, add_station_field,
                    tag_value_minute, tag_value_second, detailed_interval_secs
                )
                if not minute_history_enabled or chan_num in excluded_detail_channel_numbers:
                    watts = float(minutes_in_an_hour * watts_in_a_kw) * kwh_usage
                    timestamp = stop_time.replace(second=0)
                    usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, tag_value_minute, 
                                                             influx_version, tag_name, add_station_field))
                elif chan_num not in excluded_detail_channel_numbers and history_start_time is None:
                    no_data_flag = True
                    while no_data_flag:
                        # Collect minutes history (if neccessary, never during history collection)
                        logger.info('Get minute details; device="{}"; start="{}"; stop="{}"'.format(chan_name, minute_history_start_time, stop_time_min))
                        usage, usage_start_time = account['vue'].get_chart_usage(chan, minute_history_start_time, stop_time_min, scale=Scale.MINUTE.value, unit=Unit.KWH.value)
                        usage_start_time = usage_start_time.replace(second=0,microsecond=0)
                        index = 0
                        for kwh_usage in usage:
                            if kwh_usage is None:
                                index += 1
                                continue
                            no_data_flag = False  # Got at least one datapoint.  Set boolean value so we don't loop back
                            timestamp = usage_start_time + datetime.timedelta(minutes=index)
                            watts = float(minutes_in_an_hour * watts_in_a_kw) * kwh_usage
                            usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, tag_value_minute,
                                                                     influx_version, tag_name, add_station_field))
                            index += 1
                        if no_data_flag: 
                            # Opps!  No data points found for the time interval in question ('None' returned for ALL values)
                            # Move up the time interval to the next "batch" timeframe
                            if stop_time_min < stop_time.replace(second=0, microsecond=0):
                                foo_current_interval = int((stop_time_min - minute_history_start_time).total_seconds())
                                minute_history_start_time = minute_history_start_time + datetime.timedelta(seconds=foo_current_interval)
                                # Make sure we don't go beyond the global stopTime
                                minute_history_start_time = min(minute_history_start_time, stop_time.replace(second=0,microsecond=0))
                                stop_time_min = stop_time_min + datetime.timedelta(seconds=foo_current_interval)
                                # Make sure we don't go beyond the global stopTime
                                stop_time_min = min(stop_time_min, stop_time.replace(second=0, microsecond=0))
                            else: # Time to break out of the loop; looks like the device in question is offline 
                                no_data_flag = False
                        minute_history_enabled = False
            elif point_type == tag_value_day or point_type == tag_value_hour:
                watts = kwh_usage * 1000
                timestamp = history_start_time
                usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, point_type,
                                                         influx_version, tag_name, add_station_field))

        if chan_num in excluded_detail_channel_numbers:
            continue

        if collect_details and detailed_seconds_enabled and history_start_time is None:
            # Collect seconds (once per hour, never during history collection)
            sec_history_start_time, stop_time_sec, second_history_enabled = get_last_db_timestamp(
                device_name, chan_name, tag_value_second, detailed_start_time, stop_time, detailed_seconds_enabled,
                influx_version, bucket, query_api, influx, tag_name, add_station_field,
                tag_value_minute, tag_value_second, detailed_interval_secs
            )
            logger.verbose('Get second details; device="{}"; start="{}"; stop="{}"'.format(chan_name, sec_history_start_time, stop_time_sec), True)
            usage, usage_start_time = account['vue'].get_chart_usage(chan, sec_history_start_time, stop_time_sec, scale=Scale.SECOND.value, unit=Unit.KWH.value)
            usage_start_time = usage_start_time.replace(microsecond=0)
            index = 0
            for kwh_usage in usage:
                if kwh_usage is None:
                    index += 1
                    continue
                timestamp = usage_start_time + datetime.timedelta(seconds=index)
                watts = float(seconds_in_a_minute * minutes_in_an_hour * watts_in_a_kw) * kwh_usage
                usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, tag_value_second,
                                                         influx_version, tag_name, add_station_field))
                index += 1

        # fetches historical Hour & Day data
        if history_start_time is not None and history_end_time is not None:
            logger.verbose('Get historic details; device="{}"; start="{}"; stop="{}"'.format(chan_name, history_start_time, history_end_time), True)
            #Hours
            usage, usage_start_time = account['vue'].get_chart_usage(chan, history_start_time, history_end_time, scale=Scale.HOUR.value, unit=Unit.KWH.value)
            usage_start_time = usage_start_time.replace(minute=0, second=0, microsecond=0)
            index = 0
            for kwh_usage in usage:
                if kwh_usage is None:
                    index += 1
                    continue
                timestamp = usage_start_time + datetime.timedelta(hours=index)
                watts = kwh_usage * 1000
                usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, tag_value_hour,
                                                         influx_version, tag_name, add_station_field))
                index += 1
            #Days
            usage, usage_start_time = account['vue'].get_chart_usage(chan, history_start_time, history_end_time, scale=Scale.DAY.value, unit=Unit.KWH.value)
            index = 0
            for kwh_usage in usage:
                if kwh_usage is None:
                    index += 1
                    continue
                timestamp = usage_start_time.astimezone(account_time_zone) + datetime.timedelta(days=index)
                timestamp = timestamp.replace(hour=23, minute=59, second=59, microsecond=0)
                timestamp = timestamp.astimezone(pytz.UTC)
                watts = kwh_usage * 1000
                usage_data_points.append(create_data_point(account_name, device_name, chan_name, watts, timestamp, tag_value_day,
                                                         influx_version, tag_name, add_station_field))
                index += 1