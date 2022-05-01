from typing import List

import numpy as np
import pandas as pd
from scipy.stats import mode

from dataset_builder.radolan import RadolanUtility


def spatial_reduce(radolan_chunks: List[np.ma.MaskedArray], targets: List[str] = 'all', utility: RadolanUtility = None) -> pd.DataFrame:
    """
    Spatially reduce the radolan chunks clipped for the EZG to 
    target variables
    """
    # turn targets into a list
    if isinstance(targets, str):
        targets = [targets]
    
    # initialize a RadolanUtility
    if utility is None:
        utility = RadolanUtility()

    # create the data dictionary
    data = {}

    # go for each target
    if 'mean' in targets or 'all' in targets:
        data['mean'] = np.fromiter((chunk.mean() for chunk in radolan_chunks), dtype=float)
    
    if 'mode' in targets or 'all' in targets:
        data['mode'] = np.fromiter((float(mode(chunk.compressed()).mode) for chunk in radolan_chunks), dtype=float)
    
    if 'min' in targets or 'all' in targets:
        data['min'] = np.fromiter((chunk.min() for chunk in radolan_chunks), dtype=float)

    if 'max' in targets or 'all' in targets:
        data['max'] = np.fromiter((chunk.max() for chunk in radolan_chunks), dtype=float)
    
    if 'sum' in targets or 'all' in targets:
        data['sum'] = np.fromiter((chunk.sum() for chunk in radolan_chunks), dtype=float)

    # create the output dataframe
    df = pd.DataFrame(index=utility.timestamps, data=data)

    return df
