# harvest

still a mess, hold tight!!

Data harvester around wetterdienst


## docker

run docker container with correct mount-points

```bash
docker run -it --rm -v /src/EZG:<local EZG folder> -v /src/input_data:<local input folder> -v /src/output_data:<local output target> harvest
```
