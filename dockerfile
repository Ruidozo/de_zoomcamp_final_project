FROM mageai/mageai:latest

RUN pip install geopandas shapely fiona pyproj rtree kagglehub

CMD ["mage", "start", "de-zoomcamp-project"]
