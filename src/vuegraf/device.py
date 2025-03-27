"""
Device and channel-related functions for Vuegraf.
"""
import src.vuegraf.log as logger

def populate_devices(account):
    """
    Populate device and channel maps for an account.
    
    Args:
        account: The account dictionary to populate with device and channel maps.
    """
    device_id_map = {}
    account['deviceIdMap'] = device_id_map
    channel_id_map = {}
    account['channelIdMap'] = channel_id_map
    devices = account['vue'].get_devices()
    for device in devices:
        device = account['vue'].populate_device_properties(device)
        device_id_map[device.device_gid] = device
        for chan in device.channels:
            key = '{}-{}'.format(device.device_gid, chan.channel_num)
            if chan.name is None and chan.channel_num == '1,2,3':
                chan.name = device.device_name
            channel_id_map[key] = chan
            logger.info('Discovered new channel: {} ({})'.format(chan.name, chan.channel_num))

def lookup_device_name(account, device_gid):
    """
    Look up a device name by its ID.
    
    Args:
        account: The account dictionary containing the device map.
        device_gid: The device ID to look up.
        
    Returns:
        The device name, or the device ID as a string if not found.
    """
    if device_gid not in account['deviceIdMap']:
        populate_devices(account)

    device_name = '{}'.format(device_gid)
    if device_gid in account['deviceIdMap']:
        device_name = account['deviceIdMap'][device_gid].device_name
    return device_name

def lookup_channel_name(account, chan):
    """
    Look up a channel name.
    
    Args:
        account: The account dictionary containing the device map.
        chan: The channel object to look up.
        
    Returns:
        The channel name.
    """
    if chan.device_gid not in account['deviceIdMap']:
        populate_devices(account)

    device_name = lookup_device_name(account, chan.device_gid)
    name = '{}-{}'.format(device_name, chan.channel_num)

    try:
        num = int(chan.channel_num)
        if 'devices' in account:
            for device in account['devices']:
                if 'name' in device and device['name'] == device_name:
                    if 'channels' in device:
                        if isinstance(device['channels'], list) and len(device['channels']) >= num:
                            name = device['channels'][num - 1]
                            break
                        elif isinstance(device['channels'], dict):
                            name = device['channels'][str(num)]
                            break
    except:
        if chan.channel_num == '1,2,3':
            name = device_name

    return name