
import numpy as np
import geopandas as gpd
from shapely.geometry import Point


class GeoHelper:
	def __init__(self, crs, gdf_1 = None, gdf_2 = None, gdf_3 = None):
		self.crs = crs
		self.gdf_level_1 = gdf_1
		self.gdf_level_2 = gdf_2
		self.gdf_level_3 = gdf_3
	
	
	def search_nuts_1(self, lng, lat):
		return GeoHelper.search_geo(self.gdf_level_1, lng, lat, self.crs)
	
	def search_nuts_2(self, lng, lat):
		return GeoHelper.search_geo(self.gdf_level_2, lng, lat, self.crs)
	
	def search_nuts_3(self, lng, lat):
		return GeoHelper.search_geo(self.gdf_level_3, lng, lat, self.crs)
	
	
	@staticmethod
	def search_geo(gdf, lng, lat, crs):
		if gdf is not None:
			target_series = gpd.GeoSeries([Point(lng, lat)] * gdf.shape[0], crs=crs)
			res = gdf['geometry'].contains(target_series)
			idx = np.where(res == True)		# returns tuple, first element is array of indexes
			if (len(idx[0]) > 0):
				row = gdf.iloc[idx]
				return (str(row.iloc[0]['CODE']), str(row.iloc[0]['NAME']))
		
		return (None, None)
	
	
	@staticmethod
	def average_coord(coords):
		lng = 0.0
		lat = 0.0
		points = 0
		for point in coords:
			lng += point[0]
			lat += point[1]
			points += 1
		lng /= points
		lat /= points
		return (lng,lat)
	
	
	@staticmethod
	def coords_object(lat, lng):
		if lat and lng:
			return { "lat": lat, "lon": lng}
		return None
	