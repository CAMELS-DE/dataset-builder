from typing import List
import os
import json

from harvest.ezg import EZG
from harvest.radolan import RadolanUtility
from harvest.reducers.station import transpose_station_data
from harvest.reducers.radolan import spatial_reduce


def main(path: str, save_path: str, if_exists: str = 'skip', omit_dwd_stations: bool = False, omit_radolan_data: bool = False, ezg_name='LANGNAME', **kwargs):
    # load ezg data
    all_ezgs = EZG.from_file(path)

    # hot load RADOLAN
    if not omit_radolan_data:
        utility = RadolanUtility()

        # find params in kwargs
        params = {k.replace('radolan_'): v for k, v in kwargs.items() if k.startswith('radolan_')}
        utility._set_request_parameters(**params)

        # load
        utility._load_data()

    for ezg in all_ezgs:
        # check if this ezg already exists
        name = ezg[ezg_name]
        if os.path.exists(os.path.join(save_path, name)):
            if if_exists == 'skip':
                continue
        else:
            # create the directory
            os.mkdir(os.path.join(save_path, name))
        
        # get station data
        if not omit_dwd_stations:
            if not ezg.get_dwd_within_ezg().empty:
                stations = ezg.get_dwd_within_ezg()
            elif not ezg.get_dwd_around_centroid(kwargs.get('distance', 15), kwargs.get('unit', 'km')).empty:
                stations = ezg.get_dwd_around_centroid(kwargs.get('distance', 15), kwargs.get('unit', 'km'))
            else:
                stations = ezg.get_dwd_by_rank(kwargs.get('n', 1))
            
            # reduce the data
            station_data = transpose_station_data(
                stations,
                variables=kwargs.get('station_variables', 'all'),
                omit_quality_flag=kwargs.get('omit_quality_flag', False),
                verbose=True
            )

            # save the data
            for param_name, df in station_data.items():
                df.to_csv(os.path.join(save_path, name, f'{param_name}.csv'), index=True)

        # get radolan data
        if not omit_radolan_data:
            # get the radolan data
            radolan_chunks = ezg.dwd_radolan_load(util=utility)

            df = spatial_reduce(radolan_chunks, kwargs.get('radolan_variables', ['sum', 'mean']))

            # save
            df.to_csv(os.path.join(save_path, name, 'radolan.csv'), index=True)
        
        # finally add the ezg as a geojson
        with open(os.path.join(save_path, name, 'ezg.json'), 'w') as f:
            json.dump(ezg._geojson, f)


if __name__ == '__main__':
    import fire
    fire.Fire(main)
