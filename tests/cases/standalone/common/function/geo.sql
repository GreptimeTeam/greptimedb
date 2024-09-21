SELECT h3_latlng_to_cell(37.76938, -122.3889, 0), h3_latlng_to_cell_string(37.76938, -122.3889, 0);;

SELECT h3_latlng_to_cell(37.76938, -122.3889, 1), h3_latlng_to_cell_string(37.76938, -122.3889, 1);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8), h3_latlng_to_cell_string(37.76938, -122.3889, 8);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 100), h3_latlng_to_cell_string(37.76938, -122.3889, 100);

SELECT h3_latlng_to_cell(37.76938, -122.3889, -1), h3_latlng_to_cell_string(37.76938, -122.3889, -1);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::Int8), h3_latlng_to_cell_string(37.76938, -122.3889, 8::Int8);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::Int16), h3_latlng_to_cell_string(37.76938, -122.3889, 8::Int16);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::Int32), h3_latlng_to_cell_string(37.76938, -122.3889, 8::Int32);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::Int64), h3_latlng_to_cell_string(37.76938, -122.3889, 8::Int64);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt8), h3_latlng_to_cell_string(37.76938, -122.3889, 8::UInt8);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt16), h3_latlng_to_cell_string(37.76938, -122.3889, 8::UInt8);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt32), h3_latlng_to_cell_string(37.76938, -122.3889, 8::UInt32);

SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64), h3_latlng_to_cell_string(37.76938, -122.3889, 8::UInt64);

SELECT h3_cell_to_string(h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64)) AS cell_str, h3_string_to_cell(h3_latlng_to_cell_string(37.76938, -122.3889, 8::UInt64)) AS cell_index;

SELECT h3_cell_center_lat(h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64)) AS cell_center_lat, h3_cell_center_lng(h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64)) AS cell_center_lng;

SELECT
    h3_cell_resolution(cell) AS resolution,
    h3_cell_base(cell) AS base,
    h3_cell_is_pentagon(cell) AS pentagon,
    h3_cell_parent(cell, 6::UInt64) AS parent,
FROM (SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64) AS cell);

SELECT h3_is_neighbour(cell1, cell2)
FROM (SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64) AS cell1, h3_latlng_to_cell(36.76938, -122.3889, 8::UInt64) AS cell2);


SELECT geohash(37.76938, -122.3889, 9);

SELECT geohash(37.76938, -122.3889, 10);

SELECT geohash(37.76938, -122.3889, 11);

SELECT geohash(37.76938, -122.3889, 100);

SELECT geohash(37.76938, -122.3889, -1);

SELECT geohash(37.76938, -122.3889, 11::Int8);

SELECT geohash(37.76938, -122.3889, 11::Int16);

SELECT geohash(37.76938, -122.3889, 11::Int32);

SELECT geohash(37.76938, -122.3889, 11::Int64);

SELECT geohash(37.76938, -122.3889, 11::UInt8);

SELECT geohash(37.76938, -122.3889, 11::UInt16);

SELECT geohash(37.76938, -122.3889, 11::UInt32);

SELECT geohash(37.76938, -122.3889, 11::UInt64);
