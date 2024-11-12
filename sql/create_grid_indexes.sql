CREATE INDEX idx_cells_64km_raster_bins ON raster_bins USING GIST(cells_64km gist__intbig_ops);
CREATE INDEX idx_cells_32km_raster_bins ON raster_bins USING GIST(cells_32km gist__intbig_ops);
CREATE INDEX idx_cells_16km_raster_bins ON raster_bins USING GIST(cells_16km gist__intbig_ops);
CREATE INDEX idx_cells_8km_raster_bins ON raster_bins USING GIST(cells_8km gist__intbig_ops);
