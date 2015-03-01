#!/bin/bash

cat ./src/main/resources/max-temperature/text/* | \
    ./src/main/python/max-temperature/map.py | \
    sort | \
    ./src/main/python/max-temperature/reduce.py