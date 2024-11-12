ALTER TABLE grid_64km_aoi DROP COLUMN IF EXISTS {layer};
ALTER TABLE grid_64km_aoi ADD COLUMN {layer} integer[];

CREATE INDEX idx_{layer}_grid_64km_aoi ON grid_64km_aoi 
  USING GIST({layer} gist__int_ops);

UPDATE grid_64km_aoi SET {layer} = array(
  SELECT bid 
  FROM raster_bins 
  WHERE intset(grid_64km_aoi.gridid_64km) && cells_64km 
    AND layer = '{layer}');

ALTER TABLE grid_32km_aoi DROP COLUMN IF EXISTS {layer};
ALTER TABLE grid_32km_aoi ADD COLUMN {layer} integer[];

CREATE INDEX idx_{layer}_grid_32km_aoi ON grid_32km_aoi 
  USING GIST({layer} gist__int_ops);

UPDATE grid_32km_aoi SET {layer} = array(
  SELECT bid 
  FROM raster_bins 
  WHERE intset(grid_32km_aoi.gridid_32km) && cells_32km 
    AND layer = '{layer}');

ALTER TABLE grid_16km_aoi DROP COLUMN IF EXISTS {layer};
ALTER TABLE grid_16km_aoi ADD COLUMN {layer} integer[];

CREATE INDEX idx_{layer}_grid_16km_aoi ON grid_16km_aoi 
  USING GIST({layer} gist__int_ops);

UPDATE grid_16km_aoi SET {layer} = array(
  SELECT bid 
  FROM raster_bins 
  WHERE intset(grid_16km_aoi.gridid_16km) && cells_16km 
    AND layer = '{layer}');

ALTER TABLE grid_8km_aoi DROP COLUMN IF EXISTS {layer};
ALTER TABLE grid_8km_aoi ADD COLUMN {layer} integer[];
 
CREATE INDEX idx_{layer}_grid_8km_aoi ON grid_8km_aoi 
  USING GIST({layer} gist__int_ops);

UPDATE grid_8km_aoi SET {layer} = array(
  SELECT bid 
  FROM raster_bins 
  WHERE intset(grid_8km_aoi.gridid_8km) && cells_8km 
    AND layer = '{layer}');

