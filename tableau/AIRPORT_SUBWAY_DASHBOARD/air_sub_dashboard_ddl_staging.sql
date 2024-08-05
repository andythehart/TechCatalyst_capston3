CREATE OR REPLACE TABLE CAPSTONE_DE.GROUP_3_SCHEMA.SUBWAY_DATA (
	Division VARCHAR,
	Line VARCHAR,
	Borough VARCHAR,
	"Station Name" VARCHAR,
	"Station Latitude" NUMBER(38,10),
	"Station Longitude" NUMBER(38,10),
	"Daytime Routes" VARCHAR,
    "Entrance Type" VARCHAR,
    Entry VARCHAR,
    "Exit Only" VARCHAR,
    Vending VARCHAR,
    Staffing VARCHAR,
    "Staff Hours" VARCHAR,
    "North South Street" VARCHAR,
    "East West Street" VARCHAR,
    Corner VARCHAR,
    "Entrance Latitude" NUMBER(38,10),
	"Entrance Longitude" NUMBER(38,10),
    entrance_georeference VARCHAR,
    station_georeference VARCHAR
);



CREATE OR REPLACE STAGE capstone_de.group_3_schema.group_3_s3_stage_subway
    STORAGE_INTEGRATION = s3_int
    URL='s3://capstone-techcatalyst-transformed/group_3/subway_data'
    FILE_FORMAT = GROUP3_CSV_FORMAT;


COPY INTO capstone_de.group_3_schema.subway_data
FROM @capstone_de.group_3_schema.group_3_s3_stage_subway
FILE_FORMAT = GROUP3_CSV_FORMAT
ON_ERROR='CONTINUE';
