DROP TABLE IF EXISTS fuentes_bioclimaticas;

CREATE TABLE fuentes_bioclimaticas (id               serial, 
									fuente           varchar(100) NOT NULL, 
									descripcion      varchar(400) NOT NULL);

CREATE INDEX idx_id_fuentes_bioclimaticas ON fuentes_bioclimaticas(id);
CREATE INDEX idx_fuente_fuentes_bioclimaticas ON fuentes_bioclimaticas(fuente);