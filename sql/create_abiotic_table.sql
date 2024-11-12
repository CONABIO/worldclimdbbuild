DROP TABLE IF EXISTS raster_bins;

CREATE TABLE raster_bins (
  tag character varying,
  layer character varying,
  icat integer,
  label character varying,
  type integer,
  coeficiente float8 NULL DEFAULT 1.0,
  unidad varchar(20) NULL
) WITH (OIDS=FALSE);

ALTER TABLE raster_bins ADD COLUMN bid serial;
ALTER sequence raster_bins_bid_seq restart WITH 300000;
UPDATE raster_bins SET bid = nextval('raster_bins_bid_seq');


