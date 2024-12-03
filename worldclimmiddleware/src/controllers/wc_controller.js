var debug = require('debug')('verbs:auth')
var verb_utils = require('./verb_utils')
var pgp = require('pg-promise')()
var config = require('../../config')

var pool = verb_utils.pool 
var pool_mallas = verb_utils.pool_mallas 


let dic_wc_select = new Map();
dic_wc_select.set('1','select array_agg(bid) as level_id, id_fuentes_bio, (fb.fuente || \' - \' || fb.descripcion) as  descripcion, fb.bins, fb.area')
dic_wc_select.set('2','select array_agg(bid) as level_id, id_fuentes_bio, layer, (fb.fuente || \' - \' || fb.descripcion) as  descripcion, fb.bins, fb.area')
dic_wc_select.set('3','select bid as level_id, tag, id_fuentes_bio, layer, (fb.fuente || \' - \' || fb.descripcion) as  descripcion, fb.bins, fb.area')

let dic_wc_group = new Map();
dic_wc_group.set('1','group by id_fuentes_bio, fb.fuente, fb.descripcion, fb.bins, fb.area')
dic_wc_group.set('2','group by id_fuentes_bio, layer, fb.fuente, fb.descripcion, fb.bins, fb.area')
dic_wc_group.set('3','')

let dic_wc_order = new Map();
dic_wc_order.set('1','order by id_fuentes_bio')
dic_wc_order.set('2','order by id_fuentes_bio, layer')
dic_wc_order.set('3','order by bid')



exports.variables = function(req, res) {

	let { } = req.body;

	// Se recomienda agregar la columna available_grids a este catalogo con ayuda de los servicios disponibles del proyecto regionmiddleware
	pool.any(`with fuentes_wc as(
				select id_fuentes_bio 
				from raster_bins rb 
				group by id_fuentes_bio 
			)
			select 1 as id, 'Fuentes Worldclim' as variable, count(*) level_size, ('{"idfuente":"string"}')::jsonb filter_fields, array[1,2,3,4,5,6]::int[] as available_grids
			from fuentes_wc
			union
			(
			with layers_wc as(
				select layer  
				from raster_bins rb 
				group by layer
				order by layer 
			)
			select 2 as id, 'Layers Worldclim' as variable, count(*) level_size, ('{"idfuente":"string","idlayer":"string"}')::jsonb filter_fields, array[1,2,3,4,5,6]::int[] as available_grids
			from layers_wc
			)
			union
			(
			with ranges_wc as(
				select id_fuentes_bio, layer, icat  
				from raster_bins rb 
				group by id_fuentes_bio,layer, icat 
				order by id_fuentes_bio, layer, icat 
			)
			select 3 as id, 'Ranges Worldclim' as variable, count(*) level_size, ('{"idfuente":"string","idlayer":"string","idrange":"string"}')::jsonb filter_fields, array[1,2,3,4,5,6]::int[] as available_grids
			from ranges_wc
			)
			order by id`, {}).then( 
		function(data) {
			// debug(data);
		res.status(200).json({
			data: data
		})
  	})
  	.catch(error => {
      debug(error)
      res.status(403).json({
      	message: "error al obtener catalogo", 
      	error: error
      })
   	});
}


exports.get_variable_byid = function(req, res) {

	let variable_id = req.params.id
	debug("variable_id: " + variable_id)

	let q = verb_utils.getParam(req, 'q', '')
	let offset = verb_utils.getParam(req, 'offset', 0)
	let limit = verb_utils.getParam(req, 'limit', 10)

	debug("q: " + q)
	debug("offset: " + offset)
	debug("limit: " + limit)

	let query_array = []
	let filter_separator = ";"
	let pair_separator = "="
	let group_separator = ","

	// q: "fuente_id = 1; layer = bio018"
	// TODO: hacer filtros segun nivel
	
	// if(q != ""){

	// 	let array_queries = q.split(filter_separator)
	// 	debug(array_queries)

	// 	if(array_queries.length == 0){
	// 		debug("Sin filtros definidos")
	// 	}
	// 	else{
	// 		array_queries.forEach((filter, index) => {

	// 			let filter_pair = filter.split(pair_separator)
	// 			debug(filter_pair)

	// 			if(filter_pair.length == 0){
	// 				debug("Filtro indefinido")
	// 			}
	// 			else{
					
	// 				let filter_param = filter_pair[0].trim()
	// 				// debug("filter_param: " + filter_param)

	// 				// TODO:Revisar por que no jala en enalce del mapa con su llave valor
	// 				// debug(dic_wc.keys())
	// 				// debug("dic_wc: " + dic_wc.get(filter_param))
					

	// 				if(valid_filters.indexOf(filter_param) == -1){
	// 					debug("Filtro invalido")
	// 				}
	// 				else{
						
	// 					if(filter_pair.length != 2){
	// 						debug("Filtro invalido por composición")
	// 					}
	// 					else{
	// 						let filter_value = filter_pair[1].trim().split(group_separator)	
							
	// 						let query_temp = "( "
	// 						filter_value.forEach((value, index) => {

	// 							if(filter_param !== "levels_id"){
	// 								value = "'" + value + "'"
	// 							}
								
	// 							if(index == 0){
	// 								query_temp = query_temp + dic_wc.get(filter_param) + " = " + value
	// 							}
	// 							else{
	// 								query_temp = query_temp + " or " + dic_wc.get(filter_param) + " = " + value + " "
	// 							}

	// 						})
	// 						query_temp = query_temp + " )"
	// 						query_array.push(query_temp)

	// 					}

	// 				}

	// 			}

	// 		})	

	// 		debug(query_array)

	// 	}

	// }


	pool.task(t => {

		val_dic_wc_select = dic_wc_select.get(variable_id)
		val_dic_wc_group = dic_wc_group.get(variable_id)
		val_dic_wc_order = dic_wc_order.get(variable_id)


		let query = `{select}
					from fuentes_bioclimaticas fb 
					join raster_bins rb 
					on fb.id = rb.id_fuentes_bio
					{group}
					{order}`

		query = query.replace("{select}", val_dic_wc_select)
		query = query.replace("{group}", val_dic_wc_group)
		query = query.replace("{order}", val_dic_wc_order)

		debug(query)


		return t.any(query).then(resp => {

			let response_array = []

			resp.forEach((item, index) => {

				var temp = {}				

				temp.id = variable_id
				temp.level_id = item.level_id
				temp.data = {}
				if(variable_id == '2' || variable_id == '3')
					temp.data.layer = item.layer					
				if(variable_id == '3')
					temp.data.tag = item.tag					
				temp.data.descripcion = item.descripcion
				temp.data.bins = item.bins
				temp.data.area = item.area
				temp.data.idfuente = item.id_fuentes_bio

				response_array.push(temp)

			})

			res.status(200).json({
				data: response_array
			})


		}).catch(error => {
	      debug(error)

	      res.status(403).json({	      	
	      	error: "Error al obtener la malla solicitada", 
	      	message: "error al obtener datos"
	      })
	   	})

	}).catch(error => {
      debug(error)
      res.status(403).json({
      	message: "error general", 
      	error: error
      })
   	});
  	
}



exports.get_data_byid = async function (req, res) {

    try {
        const id = req.params.id;
        const grid_id = verb_utils.getParam(req, 'grid_id', 1);
        const levels_id = verb_utils.getParam(req, 'levels_id', []);
        const filter_names = verb_utils.getParam(req, 'filter_names', []);
        const filter_values = verb_utils.getParam(req, 'filter_values', []);

        const queryLevels = `
            SELECT bid, layer, icat, id_fuentes_bio, "label", fb.bins
            FROM raster_bins rb
            JOIN fuentes_bioclimaticas fb ON fb.id = rb.id_fuentes_bio
            WHERE bid IN ($<bids:raw>)
        `;

        // Ejecuta la primera consulta
        const levelsIds = await pool.any(queryLevels, {bids: levels_id.toString()});

        debug(levelsIds)
        
        if (levelsIds.length === 0) {
            return res.status(404).json({ message: "No se encontraron niveles." });
        }

        let queryBioParts = [];

		// levelsIds.forEach((item, index) => {
		for(item of levelsIds){

            const tableBio = `${item.layer}_q${item.bins}`;
            const layer = item.layer;
            const icat = item.icat;
            const bid = item.bid;

            // debug("tableBio: " + tableBio)
            // debug("layer: " + layer)
            // debug("icat: " + icat)
            // debug("bid: " + bid)

            const query_temp = `
                SELECT $<bid:raw> as bid, ST_AsText(ST_SetSRID(ST_Union(the_geom),4326)) as union_geom
                FROM $<tableBio:raw>
                WHERE categoria = '$<layer:raw>' AND icat = $<icat:raw>`

            const poligonResult = await pool.one(query_temp, {bid: bid, tableBio: tableBio, layer: layer, icat:icat});

            queryBioParts.push(poligonResult)


        }

        // debug(queryBioParts)

        let response_array = []

        // queryBioParts.forEach((poligonResult, index) => {
        for(poligonResult of queryBioParts){

        	
        	query_temp = `with intersectCells as (
							select $<bid:raw> as bid, g.gridid_64km as cell
							from  grid_64km_aoi as g
							join (
								select 
								ST_SetSRID(ST_GeomFromText('$<poligonResult:raw>'),4326) as geom
								) as p
							ON ST_Intersects(g.the_geom, p.geom)
						)
						select bid, array_agg(cell) as cells
						from intersectCells
						group by bid`

			inersectResult = await pool_mallas.one(query_temp, {poligonResult: poligonResult.union_geom, bid: poligonResult.bid});

			response_array.push({
				id: id,
				grid_id: grid_id,
				level_id: inersectResult.bid,
				cells: inersectResult.cells,
				n: inersectResult.cells.length
			})
        	

        }

		res.status(200).json(
			response_array
		)


    } catch (error) {
        debug(error);
        res.status(500).json({ error: "Error al procesar la solicitud.", message: error.message });
    }

};

