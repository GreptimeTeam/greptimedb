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

SELECT h3_cell_center_latlng(h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64)) AS cell_center;

SELECT
    h3_cell_resolution(cell) AS resolution,
    h3_cell_base(cell) AS base,
    h3_cell_is_pentagon(cell) AS pentagon,
    h3_cell_parent(cell, 6::UInt64) AS parent,
    h3_cell_to_children(cell, 10::UInt64) AS children,
    h3_cell_to_children_size(cell, 10) AS children_count,
    h3_cell_to_child_pos(cell, 6) AS child_pos,
    h3_child_pos_to_cell(25, cell, 11) AS child
FROM (SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64) AS cell);

SELECT
    h3_grid_disk(cell, 0) AS current_cell,
    h3_grid_disk(cell, 3) AS grids,
    h3_grid_disk_distances(cell, 3) AS all_grids,
FROM (SELECT h3_latlng_to_cell(37.76938, -122.3889, 8::UInt64) AS cell);

SELECT
    h3_grid_distance(cell1, cell2) AS distance,
    h3_grid_path_cells(cell1, cell2) AS path_cells,
    h3_distance_sphere_km(cell1, cell2) AS sphere_distance,
    h3_distance_degree(cell1, cell2) AS euclidean_distance,
FROM
    (
      SELECT
          h3_string_to_cell('86283082fffffff') AS cell1,
          h3_string_to_cell('86283470fffffff') AS cell2
    );

SELECT
    h3_cells_contains('86283470fffffff,862834777ffffff, 862834757ffffff, 86283471fffffff, 862834707ffffff', '8b283470d112fff') AS R00,
    h3_cells_contains('86283470fffffff,862834777ffffff, 862834757ffffff, 86283471fffffff, 862834707ffffff', 604189641792290815) AS R01,
    h3_cells_contains('86283470fffffff,862834777ffffff, 862834757ffffff, 86283471fffffff, 862834707ffffff', 626707639343067135) AS R02;

SELECT
    h3_cells_contains(['86283470fffffff', '862834777ffffff', '862834757ffffff', '86283471fffffff', '862834707ffffff'], '86283472fffffff') AS R10,
    h3_cells_contains(['86283470fffffff', '862834777ffffff', '862834757ffffff', '86283471fffffff', '862834707ffffff'], '8b283470d112fff') AS R11,
    h3_cells_contains(['86283470fffffff', '862834777ffffff', '862834757ffffff', '86283471fffffff', '862834707ffffff'], 626707639343067135) AS R12;

SELECT
    h3_cells_contains([604189641255419903, 604189643000250367, 604189642463379455, 604189641523855359, 604189641121202175], '8b283470d112fff') AS R20,
    h3_cells_contains([604189641255419903, 604189643000250367, 604189642463379455, 604189641523855359, 604189641121202175], 604189641792290815) AS R21,
    h3_cells_contains([604189641255419903, 604189643000250367, 604189642463379455, 604189641523855359, 604189641121202175], 626707639343067135) AS R22;


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

SELECT geohash_neighbours(37.76938, -122.3889, 11);

WITH cell_cte AS (
  SELECT s2_latlng_to_cell(37.76938, -122.3889) AS cell
)
SELECT cell,
       s2_cell_to_token(cell),
       s2_cell_level(cell),
       s2_cell_parent(cell, 3)
FROM cell_cte;

SELECT json_encode_path(37.76938, -122.3889, 1728083375::TimestampSecond);

SELECT json_encode_path(lat, lon, ts)
FROM(
        SELECT 37.76938 AS lat, -122.3889 AS lon, 1728083375::TimestampSecond AS ts
        UNION ALL
        SELECT 37.76928 AS lat, -122.3839 AS lon, 1728083373::TimestampSecond AS ts
        UNION ALL
        SELECT 37.76930 AS lat, -122.3820 AS lon, 1728083379::TimestampSecond AS ts
        UNION ALL
        SELECT 37.77001 AS lat, -122.3888 AS lon, 1728083372::TimestampSecond AS ts
);
