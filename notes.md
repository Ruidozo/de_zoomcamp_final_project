working:

docker run -it \
-p 6789:6789 \
-v $(pwd)/mage_data:/home/src \
mageai/mageai:latest
