docker run -it \
-p 6789:6789 \
-v $(pwd)/mage_data:/home/src \
-v $(pwd)/secrets:/home/src/secrets \
-v $(pwd)/.env:/home/src/.env \
mageai/mageai:latest