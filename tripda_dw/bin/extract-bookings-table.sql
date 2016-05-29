SELECT
	DISTINCT(tb.id) AS id,
	tl.id AS related_trip_id,
	tc.id || '-' || date(tl.departure_datetime) || '-' ||(CASE WHEN tlc.is_return_trip IS TRUE THEN 1 ELSE 0 END) AS trip_id,
    timezone(departure_city.timezone, tc.created_at) AS  trip_created_at,
    timezone(departure_city.timezone, tl.departure_datetime) AS trip_departure_datetime,
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
	(CASE WHEN departure_country.name = 'United States' THEN 1.60934*tlc.distance ELSE tlc.distance END) AS trip_distance,
	(CASE WHEN tl.is_canceled IS TRUE THEN 1 ELSE 0 END) AS trip_canceled,
	tc.user_id AS driver_id,
	tb.user_id AS passenger_id,
	tb.number_of_seats AS seats_booked,
    timezone(departure_city.timezone, tb.created_at) AS  booking_created_at,
	(CASE WHEN tb.approved_at IS NOT NULL THEN 1 ELSE 0 END) AS is_approved,
	(CASE WHEN tb.canceled_at IS NOT NULL THEN 1 ELSE 0 END) AS booking_canceled,
	(CASE WHEN tb.rejected_at IS NOT NULL THEN 1 ELSE 0 END) AS booking_rejected,
	tb.price_per_seat AS price,
	(CASE WHEN ur.ratings IS NOT NULL THEN ur.ratings ELSE 0 END) AS ratings
FROM
	trip_bookings tb
	JOIN trip_legs tl ON tl.id = tb.trip_leg_id
	JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id
	JOIN trip_configurations tc ON tc.id = tlc.trip_configuration_id
	JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                 
	JOIN cities AS destination_city ON destination_city.id = tlc.destination_city_id                 
	JOIN states AS departure_state ON departure_state.id = departure_city.state_id                 
	JOIN states AS destination_state ON destination_state.id = destination_city.state_id                 
	JOIN countries AS departure_country ON departure_country.id = departure_state.country_id                 
	JOIN countries AS destination_country ON destination_country.id = destination_state.country_id
	JOIN users AS drivers ON drivers.id = tc.user_id
	JOIN users AS passengers ON passengers.id = tb.user_id
	LEFT JOIN (SELECT
				ur.trip_booking_id,
				count(ur.trip_booking_id) AS ratings
						FROM 
							user_ratings ur
						GROUP BY
							ur.trip_booking_id
						ORDER BY
							ur.trip_booking_id) ur ON ur.trip_booking_id = tb.id
WHERE
	drivers.blocked IS false
	and passengers.blocked IS false
