"""
DWD station data reducer
"""
from typing import Dict, List, Union
from collections import defaultdict

import pandas as pd


# map the official DWD parameter names to shortcuts
VARIABLES = {
    'humidity': 'humidity',
    'temperature': 'temperature_air_mean_200',
    'temp': 'temperature_air_mean_200',
}


def transpose_station_data(raw_download, variables: Union[str, List[str]] = 'all', omit_quality_flag: bool = False, verbose: bool = False) -> Dict[str, pd.DataFrame]:
    """
    """
    # create the list of variables
    if variables == 'all':
        variables = list(set(VARIABLES.values()))
    else:
        variables = [VARIABLES[v] if v in VARIABLES else v for v in variables if v in VARIABLES.keys() or v in VARIABLES.values()]

    # create the container for tidy data
    tidy = defaultdict(lambda: pd.DataFrame())

    if verbose:
        print(f'Processing {len(raw_download)} stations.')

    # iterate over the raw data
    for data_download in raw_download.values.query():
        # group
        for param_name, grp in data_download.df.groupby('parameter'):
            # check for non-empty subsets
            if grp.empty:
                if verbose:
                    print(f'[Skip]: {param_name} empty' )    
                continue

            # there shall be data
            station_id = grp.station_id.unique()[0]
            data={station_id: grp.value.values}
            if not omit_quality_flag:
                data[f'quality_{station_id}'] = grp.quality.values
            
            # build the dataframe
            df = pd.DataFrame(index=grp.date, data=data).copy()

            # merge to other stations if any 
            if tidy[param_name].empty:
                tidy[param_name] = df
            else:
                tidy[param_name] = pd.merge(tidy[param_name], df, how='outer', left_index=True, right_index=True)

    if verbose:
        print(f"Processed {len(tidy)} variables: {','.join(tidy.keys())}")

    return tidy
