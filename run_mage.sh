docker run -it \
  -p 6789:6789 \
  -v /home/odiurdigital/de_zoomcamp_final_project/mage_workspace:/home/src/zoomcamp-final-project \
  mageai/mageai:latest mage start --project-uuid=/home/src/zoomcamp-final-project
