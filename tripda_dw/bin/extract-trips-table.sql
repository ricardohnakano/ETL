SELECT
	tl.id AS id,
	tc.id AS tc_id,
	tc.id || '-' || date(tl.departure_datetime) || '-' ||(CASE WHEN tlc.is_return_trip IS TRUE THEN 1 ELSE 0 END) AS trip_id,
	(CASE WHEN (tlc.departure_index = 0 AND tlc.destination_index = -1) THEN 1 ELSE 0 END) AS main_leg,
	'www.tripda' || (CASE WHEN departure_country.name = 'United States'  THEN '.com/ride/' 		       
								WHEN departure_country.name = 'Brasil' THEN '.com.br/carona/'  		       
								WHEN departure_country.name = 'Colombia' THEN '.com.co/viaje-compartido/' 		       
								WHEN departure_country.name = 'Philippines' THEN '.com.ph/ride/' 		       
								WHEN departure_country.name = 'Argentina' THEN '.com.ar/viaje-compartido/' 		       
								WHEN departure_country.name = 'Malaysia' THEN '.com.my/ride/' 		       
								WHEN departure_country.name = 'Taiwan' THEN '.com.tw/ride/' 		       
								WHEN departure_country.name = 'Mexico' THEN '.com.mx/viaje-compartido/' 		       
								WHEN departure_country.name = 'Singapore' THEN '.com.sg/ride/' 		       
								WHEN departure_country.name = 'Chile' THEN '.cl/viaje-compartido/'  
								WHEN departure_country.name = 'Uruguay' THEN '.com.uy/viaje-compartido/'		       
								WHEN departure_country.name = 'India' THEN '.in/ride/' 
								WHEN departure_country.name = 'Pakistan' THEN '.com.pk/ride/' END)   || tl.guid AS trip_link,
	tc.user_id AS driver_id,
	timezone(departure_city.timezone, tc.created_at) AS  trip_created_at,
	timezone(departure_city.timezone, tl.departure_datetime) AS trip_departure_datetime,
	tlc.departure_address AS departure_address,
	tlc.destination_address AS destination_address,
	(CASE WHEN tlc.departure_address_reverse->>'city' <> '' THEN tlc.departure_address_reverse->>'city' ELSE departure_city.name END)  AS departure_city,
	(CASE WHEN tlc.destination_address_reverse->>'city' <> '' THEN tlc.destination_address_reverse->>'city' ELSE destination_city.name END) AS destination_city,
	(CASE WHEN tlc.departure_address_reverse->>'region' <> '' THEN tlc.departure_address_reverse->>'region' ELSE departure_state.name END) AS departure_state,
	(CASE WHEN tlc.destination_address_reverse->>'region' <> '' THEN tlc.destination_address_reverse->>'region' ELSE destination_state.name END) AS destination_state,  
	departure_country.name AS departure_country,      
	destination_country.name AS destination_country,  
	(CASE WHEN departure_country.name = 'Argentina'  THEN 'Argentina + Uruguay' 		       
			WHEN departure_country.name = 'Uruguay' THEN 'Argentina + Uruguay'
			WHEN departure_country.name = 'Malaysia' THEN 'Malaysia + Singapore' 
			WHEN departure_country.name = 'Singapore' THEN 'Malaysia + Singapore' 
			ELSE departure_country.name END) AS country,  
	tc.seats_offered AS seats_offered,
	(CASE WHEN departure_country.name = 'United States' THEN 1.60934*tlc.distance ELSE tlc.distance END) AS trip_distance,
	tlc.suggested_price AS suggested_price,
	tlc.price AS price,
	(CASE WHEN tl.is_canceled IS TRUE THEN 1 ELSE 0 END) AS trip_canceled,
	(CASE WHEN tc.auto_accept_passengers IS TRUE THEN 1 ELSE 0 END) AS auto_accept,
	(CASE WHEN tc.comment IS NOT NULL THEN 1 ELSE 0 END) AS trip_comment,
	(CASE WHEN tm.driver_message IS NOT NULL THEN tm.driver_message ELSE 0 END) AS driver_message,
	(CASE WHEN tm.pax_message IS NOT NULL THEN tm.pax_message ELSE 0 END) AS pax_message,
	(CASE WHEN tc.is_recurring IS TRUE THEN 1 ELSE 0 END) AS recurrent,
	(CASE WHEN tc.ladies_only IS TRUE THEN 1 ELSE 0 END) AS ladies_only,
	(CASE WHEN tc.is_return_trip IS TRUE THEN 1 ELSE 0 END) AS is_return,
	(CASE WHEN tb.seats_booked IS NOT NULL THEN tb.seats_booked ELSE 0 END) AS seats_booked,
	(CASE WHEN tb.seats_approved IS NOT NULL THEN tb.seats_approved ELSE 0 END) AS seats_approved,
	(CASE WHEN tb.seats_canceled IS NOT NULL THEN tb.seats_canceled ELSE 0 END) AS seats_canceled,
	(CASE WHEN tb.seats_rejected IS NOT NULL THEN tb.seats_rejected ELSE 0 END) AS seats_rejected,
	promotion_codes.code AS promo_code,
	promotion_codes.description AS description_code
FROM trip_configurations tc
		JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
		JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id)
		JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                 
		JOIN cities AS destination_city ON destination_city.id = tlc.destination_city_id                 
		JOIN states AS departure_state ON departure_state.id = departure_city.state_id                 
		JOIN states AS destination_state ON destination_state.id = destination_city.state_id                 
		JOIN countries AS departure_country ON departure_country.id = departure_state.country_id                 
		JOIN countries AS destination_country ON destination_country.id = destination_state.country_id
		JOIN users AS drivers ON drivers.id = tc.user_id
		LEFT JOIN trip_promotion_code ON trip_promotion_code.trip_configuration_id = tc.id
		LEFT JOIN promotion_codes ON trip_promotion_code.promotion_code_id = promotion_codes.id
		LEFT JOIN (SELECT 
					tb.trip_leg_id AS id,
					sum(tb.number_of_seats) AS seats_booked,
					sum((CASE WHEN tb.approved_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END)) AS seats_approved,
					sum((CASE WHEN tb.canceled_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END)) AS seats_canceled,
					sum((CASE WHEN tb.rejected_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END)) AS seats_rejected
				FROM
					trip_bookings AS tb 
				GROUP BY
					tb.trip_leg_id) tb ON tb.id = tl.id
		LEFT JOIN (SELECT
					tl.id,
					sum(CASE WHEN m.sender_id = tc.user_id THEN 1 ELSE 0 END) AS driver_message,
					sum(CASE WHEN m.sender_id = tc.user_id THEN 0 ELSE 1 END) AS pax_message
				FROM
					trip_configurations tc
					JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
					JOIN trip_legs AS tl ON tl.trip_leg_configuration_id = tlc.id
					JOIN messages m ON m.trip_leg_id = tl.id
				GROUP BY
					tl.id) tm ON tm.id = tl.id
WHERE
	drivers.blocked IS false
ORDER BY
	trip_id