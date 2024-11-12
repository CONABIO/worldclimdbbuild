-- Genera datos 64 km 
UPDATE 
  raster_bins t0 
SET 
  cells_64km = array_remove(uniq(sort(t1.cells_64km)), NULL) 
FROM (
  SELECT 
    foo.icat as icat,
    aggr_array_cat(ARRAY[foo.gridid_64km]::integer[]) AS cells_64km 
  FROM (
    SELECT 
      (foo1.pvc).value as icat,
      foo1.gridid_64km 
    FROM (
      SELECT 
        ST_ValueCount(runion.urast,1) as pvc, 
        gridid_64km 
      FROM (
        SELECT 
          ST_Clip(rast,the_geom) as urast,
          gridid_64km 
        FROM 
          {layer}, grid_64km_aoi 
        WHERE ST_Intersects(rast,the_geom)
      ) as runion
    ) as foo1
  ) AS foo GROUP BY foo.icat 
) AS t1 
WHERE 
  t1.icat = t0.icat AND 
  t0.layer = '{layer}';

-- Genera datos 32 km 
UPDATE 
  raster_bins t0 
SET cells_32km = array_remove(uniq(sort(t1.cells_32km)),NULL) 
FROM (
  SELECT 
    foo.icat as icat,
    aggr_array_cat(ARRAY[foo.gridid_32km]::integer[]) AS cells_32km 
  FROM (
    SELECT 
      (foo1.pvc).value as icat,
      foo1.gridid_32km 
    FROM (
      SELECT 
        ST_ValueCount(runion.urast,1) as pvc, 
        gridid_32km 
      FROM (
        SELECT 
          ST_Clip(rast,the_geom) as urast,
          gridid_32km 
        FROM 
          {layer}, grid_32km_aoi 
        WHERE 
          ST_Intersects(rast,the_geom)
      ) as runion
    ) as foo1
  ) AS foo GROUP BY foo.icat 
) AS t1 
WHERE 
  t1.icat = t0.icat AND 
  t0.layer = '{layer}';

-- Genera datos 16 km 
UPDATE 
  raster_bins t0 
SET 
  cells_16km = array_remove(uniq(sort(t1.cells_16km)),NULL) 
FROM (
  SELECT 
    foo.icat as icat,
    aggr_array_cat(ARRAY[foo.gridid_16km]::integer[]) AS cells_16km 
  FROM (
    SELECT 
      (foo1.pvc).value as icat,
      foo1.gridid_16km 
    FROM (
      SELECT 
        ST_ValueCount(runion.urast,1) as pvc, 
        gridid_16km 
      FROM (
        SELECT 
          ST_Clip(rast,the_geom) as urast,
          gridid_16km 
        FROM 
          {layer},grid_16km_aoi 
        WHERE 
          ST_Intersects(rast,the_geom)
      ) as runion
    ) as foo1
  ) AS foo GROUP BY foo.icat 
) AS t1 
WHERE 
  t1.icat = t0.icat AND 
  t0.layer = '{layer}';

-- Genera datos 8 km
UPDATE 
  raster_bins t0 
SET 
  cells_8km = array_remove(uniq(sort(t1.cells_8km)), NULL) 
FROM (
  SELECT 
    foo.icat as icat,aggr_array_cat(ARRAY[foo.gridid_8km]::integer[]) AS cells_8km 
  FROM (
    SELECT 
      (foo1.pvc).value as icat,foo1.gridid_8km 
    FROM (
      SELECT 
        ST_ValueCount(runion.urast,1) as pvc, 
        gridid_8km 
      FROM (
        SELECT 
          ST_Clip(rast,the_geom) as urast,
          gridid_8km 
        FROM 
          {layer},grid_8km_aoi 
        WHERE 
          ST_Intersects(rast,the_geom)
      ) as runion
    ) as foo1
  ) AS foo GROUP BY foo.icat 
) AS t1 
WHERE 
  t1.icat = t0.icat AND 
  t0.layer = '{layer}';

DROP TABLE IF EXISTS {layer};
