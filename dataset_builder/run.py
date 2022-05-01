import os
import glob
import json
import os.path.join as pjoin
from datetime import datetime as dt
from datetime import timedelta as td
from dateutil.parser import parse

from wetterdienst.provider.dwd import observation, radar
import pandas as pd

from dataset_builder.ezg import EZG
from dataset_builder.radolan import RadolanUtility
from dataset_builder.reducers.station import transpose_station_data

# check if this file is running in a container
if os.path.exists('./.incontainer'):
    BASEPATH = '/src'
else:
    BASEPATH = os.path.abspath(pjoin(os.path.dirname(__file__), '..'))


# default settings
DEFAULTS = {
    'ezg_dir': pjoin(BASEPATH, 'EZG'),
    'output_dir': pjoin(BASEPATH, 'output_data'),
    'input_dir': pjoin(BASEPATH, 'input_data'),
    'station_distance': 15,
    'station_closest_n': 1,
    'omit_quality_flag': True,
    'dwd_resolution': 'DAILY',
    'dwd_parameter': ['CLIMATE_SUMMARY'],
    'dwd_period': ['HISTORICAL', 'RECENT'],
    'radar_parameter': 'RADOLAN_CDC',
    'radar_period': ['HISTORICAL', 'RECENT'],
    'radar_resolution': 'DAILY',
    'radar_end_date': 'now',
    'radar_start_date': None,
    'name_property': ['FG_ID', 'LANGNAME'],          # adjust this!
    'if_exists': 'skip',
}

def __build_kw(**kwargs):
    # replace the constants for wetterdienst
    # DWD STATION
    # resolution
    dwd_reoslution = kwargs.get('dwd_resolution', DEFAULTS['dwd_resolution'])
    kwargs['dwd_resolution'] = getattr(observation.DwdObservationResolution, dwd_reoslution)

    # parameter
    dwd_parameter = kwargs.get('dwd_parameter', DEFAULTS['dwd_parameter'])
    if not isinstance(dwd_parameter, list):
        dwd_parameter = [dwd_parameter]
    kwargs['dwd_parameter'] = [getattr(observation.DwdObservationDataset, par) for par in dwd_parameter]

    # period
    dwd_period = kwargs.get('dwd_period', DEFAULTS['dwd_period'])
    if not isinstance(dwd_period, list):
        dwd_period = [dwd_period]
    kwargs['dwd_period'] = [getattr(observation.DwdObservationPeriod, per) for per in dwd_period]

    # DWD RADOLAN
    radar_parameter = kwargs.get('radar_parameter', DEFAULTS['radar_parameter'])
    if not isinstance(radar_parameter, list):
        radar_parameter = [radar_parameter]
    kwargs['radar_parameter'] = [getattr(radar.DwdRadarParameter, par) for par in radar_parameter]
    
    radar_period = kwargs.get('radar_period', DEFAULTS['radar_period'])
    if not isinstance(radar_period, list):
        radar_period = [radar_period]
    kwargs['radar_period'] = [getattr(radar.DwdRadarPeriod, per) for per in radar_period]

    radar_resolution = kwargs.get('radar_resolution', DEFAULTS['radar_resolution'])
    kwargs['radar_resolution'] = getattr(radar.DwdRadarResolution, radar_resolution)
    
    end_date = kwargs.get('radar_end_date', DEFAULTS['radar_end_date'])
    if end_date == 'now':
        end_date = dt.now()
    elif isinstance(end_date, str):
        end_date = parse(end_date)
    kwargs['radar_end_date'] = end_date

    start_date = kwargs.get('radar_start_date', DEFAULTS['radar_start_date'])
    if start_date is None:
        start_date = dt(2001, 1, 1, 00, 00, 00)
    elif isinstance(start_date, str):
        start_date = parse(start_date)
    elif isinstance(start_date, int):
        start_date = end_date - td(days=start_date)
    else:
        raise AttributeError('radar_start_date is not valid')
    kwargs['radar_start_date'] = start_date

    # name property
    name = kwargs.get('name_property', DEFAULTS['name_property'])
    if not isinstance(name, list):
        name = [name]
    kwargs['name_property'] = name

    kw = DEFAULTS.copy()
    kw.update(kwargs)
    return kw


def run(**kwargs):
    # parse the eyword arguments
    kwargs = __build_kw(**kwargs)

    # get the ezg shapes
    ezgs = []
    for fname in glob.glob(pjoin(kwargs['ezg_dir'], '*.shp')):
        ezgs.append(EZG.from_file(fname))
    for fname in glob.glob(pjoin(kwargs['ezg_dir'], '*.geojson')):
        ezgs.append(EZG.from_file(fname))

    print(f"Found {len(ezgs)} EZG shapes")

    # build the radolan utility
    utils = []
    for per in kwargs['radar_period']:
        util = RadolanUtility(
            parameter=kwargs['radar_parameter'],
            period=per,
            resolution=kwargs['radar_resolution'],
            start_date=kwargs['radar_start_date'],
            end_date=kwargs['radar_end_date']
        )
        # hot load
        util._load_data()
        utils.append(util)
    
    # MAIN LOOP
    for i, ezg in enumerate(ezgs):
        # build the name
        name = '_'.join([str(ezg.properties.get(prop, f'EZG_{i + 1}')) for prop in kwargs['name_property']])

        # check if this folder already exists
        if os.path.exists(pjoin(kwargs['output_dir'], name)):
            if kwargs['if_exists'] == 'skip':
                print(f"Skipping {name}")
                continue
            else:
                os.mkdir(pjoin(kwargs['output_dir'], name))
        
        # --------------
        # DWD stations
        for P in kwargs['dwd_parameter']:
            EZG._dwd_request_params['parameter'] = P
            data_cache = dict()
            for period in kwargs['dwd_period']:
                EZG._dwd_request_params['period'] = period

                # laod station data
                if not ezg.get_dwd_within_ezg().df.empty:
                    stations = ezg.get_dwd_within_ezg()
                elif not ezg.get_dwd_around_centroid(kwargs['station_distance'], 'km').df.empty:
                    stations = ezg.get_dwd_around_centroid(kwargs['station_distance'], 'km')
                else:
                    stations = ezg.get_dwd_by_rank(kwargs['station_closest_n'])
                
                # reduce the data
                station_data = transpose_station_data(stations, variables='all', omit_quality_flag=kwargs['omit_quality_flag'])

                # cache the data
                for param_name, df in station_data.items():
                    data_cache[param_name] = df if param_name not in data_cache else pd.concat((data_cache[param_name], df))
            
            # all periods loaded - save the data
            for param_name, df in data_cache.items():
                df.to_csv(pjoin(kwargs['output_dir'], name, f"{param_name}.csv"), index=True)

        # --------------
        # RADOLAN data
        rado_df = pd.DataFrame()
        for util in utils:
            # get the radolan chunks
            radolan_chunk = ezg.dwd_radolan_load(util=util)
            
            # reduce the data
            df = spatial_reduce(radolan_chunk, targets=['sum', 'mean'], utility=util)
            rado_df = pd.concat((rado_df, df))
        
        # save
        rado_df.to_csv(pjoin(kwargs['output_dir'], name, 'radolan.csv'), index=True)

        # finally save the EZG shape itself
        with open(pjoin(kwargs['output_dir'], name, 'ezg.geojson'), 'w') as fp:
            json.dump(ezg._geojson, fp)

if __name__ == '__main__':
    import fire
    fire.Fire(run)
