import tempfile
from datetime import datetime as dt
from datetime import timedelta as td

from wetterdienst.provider.dwd.radar import (
    DwdRadarValues,
    DwdRadarParameter,
    DwdRadarPeriod,
    DwdRadarResolution
)
import wradlib as wrl
import rasterio
from pyproj import CRS, Transformer
from dateutil.parser import parse


# helper function for frozen hashing
_h = lambda d: hash(str(frozenset(d.items())))

NOW = dt.now().replace(hour=0, minute=0, second=0, microsecond=0)

# create default
DEFAULT_REQUEST = dict(
    parameter=DwdRadarParameter.RADOLAN_CDC,
    resolution=DwdRadarResolution.HOURLY,
    period=DwdRadarPeriod.RECENT,
    start_date=NOW - td(days=10),
    end_date = NOW,
)

class RadolanUtility:
    # cache
    _request_cache = DEFAULT_REQUEST
    _request_hash = _h(DEFAULT_REQUEST)
    _dataset_cache = []
    _rasterio_cache = []
    _attribute_cache = []
    _timestamp_cache = []
    _ezg_transform = None

    # metadata
    CRS = wrl.georef.create_osr('dwd-radolan')
    GRID = wrl.georef.get_radolan_grid(900, 900)

    def __init__(self, cache_dir: str = None, **kwargs):
        self._set_request_parameters(**kwargs)

        if cache_dir is not None:
            pass

    def __getitem__(self, key: str):
        return self._request_cache[key]

    def __setitem__(self, key: str, value):
        self._set_request_parameters(**{key: value})

    def _set_request_parameters(self, **kwargs) -> None:
        # check all parameters
        if 'parameter' in kwargs:
            if isinstance(kwargs['parameter'], DwdRadarParameter):
                self._request_cache['parameter'] = kwargs['parameter']
            else:
                self._request_cache['parameter'] = getattr(DwdRadarParameter, kwargs['parameter'].upper())
        
        if 'resolution' in kwargs:
            if isinstance(kwargs['resolution'], DwdRadarResolution):
                self._request_cache['resolution'] = kwargs['resolution']
            else:
                self._request_cache['resolution'] = getattr(DwdRadarResolution, kwargs['resolution'].upper())
        
        if 'period' in kwargs:
            if isinstance(kwargs['period'], DwdRadarPeriod):
                self._request_cache['period'] = kwargs['period']
            else:
                self._request_cache['period'] = getattr(DwdRadarPeriod, kwargs['period'].upper())
            
        if 'start_date' in kwargs:
            if isinstance(kwargs['start_date'], dt):
                self._request_cache['start_date'] = kwargs['start_date']
            else:
                self._request_cache['start_date'] = parse(kwargs['start_date'])
        
        if 'end_date' in kwargs:
            if isinstance(kwargs['end_date'], dt):
                self._request_cache['end_date'] = kwargs['end_date']
            else:
                self._request_cache['end_date'] = parse(kwargs['end_date'])

        # check if any parameter has changed
        new_hash = _h(self._request_cache)
        if new_hash != self._request_hash:
            # empty caches
            self._dataset_cache = []
            self._timestamp_cache = []
            self._rasterio_cache = []
            self._attribute_cache = []
            self._ezg_transform = None

            # set new hash
            self._request_hash = new_hash

    def _load_data(self):
        # build the request
        radolan = DwdRadarValues(**{k: v for k, v in self._request_cache.items()})

        # load data
        for item in radolan.query():
            # load data
            try:
                ds, meta = wrl.io.read_radolan_composite(item.data)
            except Exception as e:
                print(f"Failed at: {item.timestamp}\n{str(e)}")
                continue
            
            # save to cache
            self._timestamp_cache.append(meta['datetime'])
            self._attribute_cache.append(meta)
            self._dataset_cache.append(ds)
        
    @property
    def raw_datasets(self):
        # if cache is empty, load data
        if len(self._dataset_cache) == 0:
            self._load_data()
        return self._dataset_cache
    
    @property
    def datasets(self):
        if len(self._rasterio_cache) == 0:
            for ds in self.raw_datasets:
                self._rasterio_cache.append(self.convert_to_rasterio(ds))
        return self._rasterio_cache
    
    @property
    def timestamps(self):
        return self._timestamp_cache

    @property
    def attributes(self):
        return self._attribute_cache
    
    def convert_to_rasterio(self, raw_data):
        """
        Convert the cached raster slice to rasterio.
        This is needed to use rasterio masking functions
        """
        # set the correct origin for RADOLAN data
        raster, xy = wrl.georef.set_raster_origin(raw_data, self.GRID, 'upper')

        # create GDAL dataset
        ds = wrl.georef.create_raster_dataset(raster, xy, self.CRS)

        # create a temporary file and open with rasterio
        with tempfile.NamedTemporaryFile('w+b') as tmp:
            wrl.io.write_raster_dataset(tmp.name, ds, 'GTiff')
            return rasterio.open(tmp.name)
    

