#!/bin/bash

./src/main/scripts/max-temperature-new-api-job.sh && \
./src/main/scripts/max-temperature-new-api-job-with-combiner.sh && \
./src/main/scripts/word-count/word-count-new-api-job-with-combiner.sh && \
./src/main/scripts/word-count/word-count-old-api-job-with-combiner.sh