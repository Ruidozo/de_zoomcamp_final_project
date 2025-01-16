FROM mageai/mageai:latest

RUN pip install geopandas shapely fiona pyproj rtree

CMD ["mage", "start", "de-zoomcamp-project"]
