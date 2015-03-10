-- US counties, population, housing and area

fs -rm -r target/output;

data_with_header = LOAD 'src/main/resources/us-census/counties.csv'
    USING PigStorage(',')
    AS (county:chararray, state:chararray, population:int, housing_units:int, total_area:double, water_area:double, land_area:double, density_pop:double, density_housing:double)
    ;

data = FILTER data_with_header BY county != 'County';
DUMP data;

counties = FOREACH data GENERATE county, state;

STORE counties INTO 'target/output';