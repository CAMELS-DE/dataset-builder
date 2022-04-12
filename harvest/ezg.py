"""
EZG utility

Used to derive a bbox from each EZG, and clip downloaded data.

"""
from typing import Callable, Tuple, Union, List
import fiona
from rasterio.mask import mask
from pyproj import CRS, Transformer
from shapely.geometry import shape, Polygon
from shapely.ops import transform
from wetterdienst.provider.dwd.observation import DwdObservationRequest, DwdObservationDataset, DwdObservationPeriod, DwdObservationResolution
from dateutil.parser import parse
import numpy as np

from .radolan import RadolanUtility


class EZG:
    _dwd_request_params = dict(
        parameter=DwdObservationDataset.CLIMATE_SUMMARY,
        resolution=DwdObservationResolution.DAILY,
        period=DwdObservationPeriod.HISTORICAL,
        start_date=None,
        end_date=None
    )

    def __init__(self, data: dict = None, path: str = None, index: int = 0, filter: Tuple[str, str] = None, crs: str = None):
        # load the geojson data
        if data is not None:
            self._geojson = data
        
        elif path is not None:
            with fiona.open(path, 'r') as collection:
                if filter is not None:
                    for feature in collection:
                        if feature['properties'].get(filter[0]) == filter[1]:
                            self._geojson = feature
                            break
                else:
                    self._geojson = collection[index]        
                
                # extract CRS and schema
                self._crs = collection._crs['init']
                self._schema = collection.schema
        else:
            raise AttributeError('EZG must be initialized with either a data dict or a path')
        
        # load the shape
        self._shape = shape(self._geojson['geometry'])

        if crs is not None:
            self._crs = crs

    @classmethod
    def from_file(cls, path: str) -> List['EZG']:
        with fiona.open(path, 'r') as collection:
            crs = collection.crs['init']
            return [cls(data=feature, crs=crs) for feature in collection]
    
    @property
    def WKT(self) -> str:
        return self._shape.wkt
    
    @property
    def WKB(self) -> bytes:
        return self._shape.wkb

    @property
    def shape(self) -> Polygon:
        return self._shape
    
    @property
    def crs(self) -> CRS:
        if self._crs is None:
            raise ValueError('Please set a CRS first')
        return CRS(self._crs)
    
    @crs.setter
    def crs(self, value: Union[str, int]):
        if isinstance(value, str):
            if not value.lower().startswith('epsg:'):
                raise ValueError('Only EPSG definitions are allowed now')
            self._crs = value
        elif isinstance(value, int):
            self._crs = f'EPSG:{value}'
        else:
            raise AttributeError('CRS must be a string or an integer')

    @property
    def bbox(self) -> Tuple[float, float, float, float]:
        return self._shape.bounds

    @property
    def properties(self) -> dict:
        if 'properties' in self._geojson:
            return self._geojson['properties']
        else:
            return {}

    def transform(self, to: str = 'EPSG:4326') -> Callable:
        # define coordinate reference systems
        src = self.crs
        tgt = CRS(to)

        # define transformer
        transformer = Transformer.from_crs(src, tgt, always_xy=True)

        return transformer.transform

    def _get_dwd_request(self, **kwargs):
        # define the request
        stations = DwdObservationRequest(**self._dwd_request_params)

        return stations

    def get_dwd_within_ezg(self):
            # get a WGS84 polygon of this EZG
        transformer = self.transform('EPSG:4326')
        poly = transform(transformer, self.shape)

        stations = self._get_dwd_request()
        return stations.filter_by_bbox(*poly.bounds)
    
    def get_dwd_around_centroid(self, distance, unit='km'):
        # get centroid and transform to WGS84
        transformer = self.transform('EPSG:4326')
        centroid = transform(transformer, self.shape.centroid)

        stations = self._get_dwd_request()
        return stations.filter_by_distance(longitude=centroid.x, latitude=centroid.y, distance=distance, unit=unit)
    
    def get_dwd_by_rank(self, n: int = 1):
        # get centroid and transform to WGS84
        transformer = self.transform('EPSG:4326')
        centroid = transform(transformer, self.shape.centroid)
        
        stations = self._get_dwd_request()
        return stations.filter_by_rank(longitude=centroid.x, latitude=centroid.y, rank=n)

    def dwd_station_data(self, distance=None, n=None, **kwargs):
        """
        """
        # make the correct request
        if distance is not None:
            stationResult = self.get_dwd_around_centroid(distance, **kwargs)
        elif n is not None:
            stationResult = self.get_dwd_by_rank(n, **kwargs)
        else:
            stationResult = self.get_dwd_within_ezg(**kwargs)
        
        # check for emtpy df
        if stationResult.df.empty:
            return
        
        # get the station data
        return [result.df.dropna() for result in stationResult.values.query() if not result.df.dropna().empty]

    def dwd_radolan_load(self, util: RadolanUtility = None):
        if util is None:
            util = RadolanUtility()

        # this takes time
        datasets = util.datasets
        metadata = util.attributes
        
        # create a transformer to meet Radolan CRS
        src_crs = self.crs
        tgt_crs = datasets[0].crs
        transformer = Transformer.from_crs(src_crs, tgt_crs, always_xy=True).transform
        
        # transform the shape
        shape = transform(transformer, self.shape)

        # get rid of the 3rd coordinate dimension as rasterio <= 1.3 can't handle that
        shape = transform(lambda x, y, z=None: (x, y), shape)

        result = []
        for dataset, meta in zip(datasets, metadata):
            cropped, _ = mask(dataset, shapes=[shape], crop=True)
            maskeddata = np.ma.masked_equal(cropped[0], meta.get('nodataflag', -9999))
            result.append(maskeddata)

        return result

    def __getitem__(self, key: str) -> Union[str, float, int]:
        return self._geojson['properties'][key]
