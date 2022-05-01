# Dataset-builder

still a mess, hold tight!!

Data harvester around wetterdienst. 


## Install

### Python Package

```bash
pip install dataset-builder
```

## docker

If you have docker installed, using the docker image is recommended.
You don't need to install anything else as the package is run in an isolated container. To persist input and output data,
you have to mount three folders to your local harddrive:
 * `/src/EZG` for the input shape or geojson files
 * `/src/input_data` for any input shapefiles, that should be intersected with EZG (soil, landuse)
 * `/src/output_data` for the final dataset. It will create a sub-folder for each feature found in `EZG`

 
run docker container with correct mount-points

```bash
docker run -it --rm -v /src/EZG:<local EZG folder> -v /src/input_data:<local input folder> -v /src/output_data:<local output target> ghcr.io/camels-de/dataset-builder
```
