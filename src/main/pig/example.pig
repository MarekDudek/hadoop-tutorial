-- Finds the maximum temperature by year
-- tu run locally

records = LOAD 'src/main/resources/pig/sample.txt' AS (year:chararray, temperature:int, quality:int);
filtered_records = FILTER records BY temperature != 9999 AND quality IN (1, 2);

grouped_records = GROUP filtered_records BY year;

max_temp = FOREACH grouped_records GENERATE group, MAX(filtered_records.temperature);

DUMP max_temp;
