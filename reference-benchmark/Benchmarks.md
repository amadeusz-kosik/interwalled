
# Benchmarks

## Scalability benchmarks
The goal of this benchmark is to measure scalability of each application and check what defines maximum size of the
input dataset. An ideal application would accept any size of the input and process it within a finite amount of time
without OOM / process killed errors.

RAM constraints are defined as follows:
- AIList / AITree / NITree: memory available for the whole docker container.
- SeQuiLa: 1:1 ratio for the driver and the executor (i.e. 16G means 8G for the executor and 8G for the driver).
- Spark SQL: 2G for the driver, the remaining memory for the executor.

### Mirror join
This benchmark measures running the join application providing the same dataset as both sides of the join. Since
intervals in the dataset are not overlapping, the output should contain the same number of rows as the input.

| Data size | RAM available |                   AIList |                   AITree | NITree |    SeQuiLa (4t, 4p) |   SeQuiLa (4t, 32p) | SeQuiLa (4t, 32p) 1/8 | Spark SQL (128p) |
|----------:|--------------:|-------------------------:|-------------------------:|-------:|--------------------:|--------------------:|----------------------:|-----------------:|
|       80M |            4G | 0m 34s - 0m 30s - 0m 04s | 0m 23s - 0m 20s - 0m 02s | killed |                 OOM |                 OOM |                   14m |            > 10m |
|       80M |            8G | 0m 31s - 0m 29s - 0m 01s | 0m 23s - 0m 20s - 0m 03s | killed |              6m 42s |              2m 12s |                   14m |            > 10m |
|       80M |           16G | 0m 30s - 0m 29s - 0m 01s | 0m 22s - 0m 20s - 0m 02s | killed |              2m 24s |              2m 12s |                   10m |            > 10m |
|       80M |           32G | 0m 34s - 0m 30s - 0m 04s | 0m 24s - 0m 20s - 0m 03s | killed |              2m 18s |              2m 18s |                   10m |            > 10m |
|       80M |           64G | 0m 30s - 0m 29s - 0m 01s | 0m 22s - 0m 20s - 0m 02s | killed |              1m 36s |              2m 06s |                    7m |            > 10m |
|      120M |            4G | 0m 49s - 0m 33s - 0m 05s |                   killed |      - |                 OOM |                 OOM |                   21m |            > 10m |
|      120M |            8G | 0m 49s - 0m 45s - 0m 04s |                   killed |      - |                 OOM |                 OOM |                   21m |            > 10m |
|      120M |           16G | 0m 49s - 0m 44s - 0m 05s |                   killed |      - |              4m 00s |              3m 18s |                   14m |            > 10m |
|      120M |           32G | 0m 49s - 0m 44s - 0m 04s |                   killed |      - |              3m 24s |              3m 18s |                     - |            > 10m |
|      120M |           64G | 0m 48s - 0m 44s - 0m 04s |                   killed |      - |              3m 06s |              3m 00s |                     - |            > 10m |
|      240M |            4G |                   killed |                        - |      - |                 OOM |                 OOM |                   OOM |            > 10m |
|      240M |            8G | 1m 55s - 1m 33s - 0m 22s |                        - |      - |                 OOM |                 OOM |                   42m |            > 10m |
|      240M |           16G | 1m 55s - 1m 33s - 0m 21s |                        - |      - |                 OOM | MaxResultSize Error |                   30m |            > 10m |
|      240M |           32G | 1m 54s - 1m 33s - 0m 21s |                        - |      - |                 OOM | MaxResultSize Error |                   34m |            > 10m |
|      240M |           64G | 1m 53s - 1m 32s - 0m 20s |                        - |      - |                 OOM | MaxResultSize Error |                     - |            > 10m |
|      320M |            4G |                   killed |                        - |      - |                 OOM |                 OOM |                   OOM |            > 10m |
|      320M |            8G | 2m 09s - 2m 02s - 0m 06s |                        - |      - |                 OOM |                 OOM |                   OOM |            > 10m |
|      320M |           16G | 2m 07s - 2m 01s - 0m 06s |                        - |      - |                 OOM | MaxResultSize Error |                1h 00m |            > 10m |
|      320M |           32G | 2m 07s - 2m 01s - 0m 06s |                        - |      - | MaxResultSize Error | MaxResultSize Error |                   41m |            > 10m |
|      320M |           64G | 2m 07s - 2m 01s - 0m 05s |                        - |      - | MaxResultSize Error | MaxResultSize Error |                   27m |            > 10m |
|      400M |            4G |                   killed |                        - |      - |                 OOM |                 OOM |                   OOM |            > 10m |
|      400M |            8G |                   killed |                        - |      - |                 OOM |                 OOM |                   OOM |            > 10m |
|      400M |           16G |                   killed |                        - |      - |                 OOM | MaxResultSize Error |                1h 12m |            > 10m |
|      400M |           32G |                   killed |                        - |      - |                 OOM | MaxResultSize Error |                   50m |            > 10m |
|      400M |           64G |                   killed |                        - |      - | MaxResultSize Error | MaxResultSize Error |                   35m |            > 10m |
|      480M |            4G |                        - |                        - |      - |                   - |                   - |                   OOM |            > 10m |
|      480M |            8G |                        - |                        - |      - |                   - |                   - |                   OOM |            > 10m |
|      480M |           16G |                        - |                        - |      - |                   - |                   - |                1h 24m |            > 10m |
|      480M |           32G |                        - |                        - |      - |                   - |                   - |                   57m |            > 10m |
|      480M |           64G |                        - |                        - |      - |                   - |                   - |                   40m |            > 10m |
|      640M |            4G |                        - |                        - |      - |                   - |                 OOM |                   OOM |            > 10m |
|      640M |            8G |                        - |                        - |      - |                   - |                 OOM |                   OOM |            > 10m |
|      640M |           16G |                        - |                        - |      - |                   - |                   - |                2h 00m |            > 10m |
|      640M |           32G |                        - |                        - |      - |                   - |                   - |                1h 42m |            > 10m |
|      640M |           64G |                        - |                        - |      - |                   - |                   - |                   55m |            > 10m |

## Asymmetrical join

| Query size | Database size | RAM available |                   AIList |                   AITree | SeQuiLa |
|-----------:|--------------:|--------------:|-------------------------:|-------------------------:|--------:|
|        80M |           80M |            4G | 0m 34s - 0m 30s - 0m 04s | 0m 22s - 0m 20s - 0m 02s |     OOM |
|        80M |           80M |            8G | 0m 31s - 0m 30s - 0m 01s | 0m 22s - 0m 20s - 0m 02s |  3m 36s |
|        80M |           80M |           16G | 0m 30s - 0m 29s - 0m 01s | 0m 22s - 0m 20s - 0m 02s |  3m 36s |
|        80M |           80M |           32G | 0m 31s - 0m 30s - 0m 01s | 0m 22s - 0m 20s - 0m 02s |  3m 30s |
|        80M |           80M |           64G | 0m 34s - 0m 30s - 0m 04s | 0m 22s - 0m 20s - 0m 02s |  2m 18s |
|        80M |          120M |            4G | 0m 40s - 0m 40s - 0m 02s | 0m 30s - 0m 27s - 0m 02s |     OOM |
|        80M |          120M |            8G | 0m 40s - 0m 39s - 0m 01s | 0m 34s - 0m 27s - 0m 07s |  4m 00s |
|        80M |          120M |           16G | 0m 46s - 0m 39s - 0m 06s | 0m 34s - 0m 27s - 0m 07s |  4m 30s |
|        80M |          120M |           32G | 0m 41s - 0m 39s - 0m 02s | 0m 30s - 0m 28s - 0m 02s |  4m 24s |
|        80M |          120M |           64G | 0m 41s - 0m 39s - 0m 01s | 0m 31s - 0m 29s - 0m 02s |  2m 24s |
|        80M |          240M |            4G | 1m 09s - 1m 07s - 0m 02s | 0m 55s - 0m 48s - 0m 07s |     OOM |
|        80M |          240M |            8G | 1m 09s - 1m 08s - 0m 02s | 0m 51s - 0m 48s - 0m 03s |  4m 42s |
|        80M |          240M |           16G | 1m 10s - 1m 08s - 0m 02s | 0m 51s - 0m 48s - 0m 03s |  5m 18s |
|        80M |          240M |           32G | 1m 14s - 1m 12s - 0m 02s | 0m 51s - 0m 48s - 0m 03s |  5m 30s |
|        80M |          240M |           64G | 1m 12s - 1m 10s - 1m 02s | 0m 51s - 0m 48s - 0m 03s |  3m 00s |
|        80M |          320M |            4G | 1m 28s - 1m 25s - 0m 03s | 1m 11s - 1m 03s - 0m 08s |     OOM |
|        80M |          320M |            8G | 1m 29s - 1m 27s - 0m 02s | 1m 07s - 1m 03s - 0m 04s |  5m 30s |
|        80M |          320M |           16G | 1m 28s - 1m 25s - 0m 02s | 1m 06s - 1m 03s - 0m 03s |  5m 12s |
|        80M |          320M |           32G | 1m 34s - 1m 26s - 0m 07s | 1m 06s - 1m 04s - 0m 02s |  5m 36s |
|        80M |          320M |           64G | 1m 28s - 1m 26s - 0m 02s | 1m 06s - 1m 03s - 0m 04s |  3m 30s |
|        80M |          480M |            4G | 2m 06s - 2m 02s - 0m 04s | 1m 38s - 1m 32s - 0m 06s |     OOM |
|        80M |          480M |            8G | 2m 06s - 2m 03s - 0m 03s | 1m 37s - 1m 32s - 0m 05s |  6m 36s |
|        80M |          480M |           16G | 2m 11s - 2m 02s - 0m 08s | 1m 36s - 1m 31s - 0m 05s |  5m 54s |
|        80M |          480M |           32G | 2m 05s - 2m 02s - 0m 03s | 1m 36s - 1m 31s - 0m 05s |  7m 12s |
|        80M |          480M |           64G | 2m 10s - 2m 02s - 0m 08s | 1m 36s - 1m 31s - 0m 05s |  4m 00s |
|        80M |          640M |            4G | 2m 46s - 2m 40s - 0m 05s | 2m 08s - 2m 00s - 0m 07s |     OOM |
|        80M |          640M |            8G | 2m 49s - 2m 40s - 0m 09s | 2m 08s - 2m 00s - 0m 08s |  7m 18s |
|        80M |          640M |           16G | 2m 50s - 2m 41s - 0m 09s | 2m 10s - 2m 00s - 0m 11s |  7m 00s |
|        80M |          640M |           32G | 2m 49s - 2m 40s - 0m 09s | 2m 05s - 1m 59s - 0m 06s |  6m 54s |
|        80M |          640M |           64G | 2m 50s - 2m 40s - 0m 10s | 2m 06s - 2m 00s - 0m 06s |  5m 00s |