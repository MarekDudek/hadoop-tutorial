#!/bin/bash

PREFIX=./src/main/scripts

$PREFIX/max-temperature/max-temperature-bash-awk.sh && \
$PREFIX/max-temperature/max-temperature-new-api-job.sh && \
$PREFIX/max-temperature/max-temperature-new-api-job-with-combiner.sh && \

$PREFIX/word-count/word-count-new-api-job-with-combiner.sh && \
$PREFIX/word-count/word-count-old-api-job-with-combiner.sh

echo All scripts ran