FROM mageai/mageai:latest

# Increase pip timeout and install packages individually
RUN pip install --default-timeout=100 geopandas \
    && pip install --default-timeout=100 shapely \
    && pip install --default-timeout=100 fiona \
    && pip install --default-timeout=100 pyproj \
    && pip install --default-timeout=100 rtree \
    && pip install --default-timeout=100 kagglehub \
    && pip install --default-timeout=100 dbt

CMD ["mage", "start", "de-zoomcamp-project"]