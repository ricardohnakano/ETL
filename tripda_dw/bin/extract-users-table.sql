SELECT
	tprinc.id,
	tprinc.guid,
	tprinc.first_name,
	tprinc.last_name,
	tprinc.birthdate,
	tprinc.age,
	tprinc.gender,
	tprinc.created_at,
	tprinc.country,
	tprinc.locale,
	tprinc.affiliate,
	tprinc.phone_number,
	tprinc.email,
	tprinc.facebook_profile,
	tprinc.pref_chat,
	tprinc.pref_music,
	tprinc.pref_smoking,
	tprinc.pref_pets,
	tprinc.pref_food,
	tprinc.share_phone,
	tprinc.share_email,
	tprinc.share_facebook,
	tprinc.share_nothing,
	tprinc.facebook_friends,
	tprinc.phone_verified,
	tprinc.email_verified,
	tsec.badges,
	tsec.ratings,
	tsec.total_rating,
	tsec.have_comments,
	top.android,
	top.ios,
	toff.first_offer,
	toff.last_offer,
	toffbook.first_driver_with_booking,
	toffbook.last_driver_with_booking,	
	tdriver.first_driver,
	tdriver.last_driver,
	toff.total_offer,
	tbook.first_booking,
	tbook.last_booking,
	tpax.first_pax,
	tpax.last_pax,
	tbook.total_booking
FROM
	(SELECT
		u.id,
		u.guid,
		u.first_name,
		u.last_name,
		u.birthdate,
		EXTRACT(YEAR FROM age(date(now()),date(u.birthdate))) AS Age,
		(CASE WHEN u.name_prefix = 1 THEN 'Male' 
				WHEN u.name_prefix = 2 OR u.name_prefix = 3 THEN 'Female'  
				WHEN u.name_prefix = 0 THEN 'Undefined' END) AS Gender,
		u.created_at AS created_at_london,
		(CASE WHEN u.locale = 'en_US'  THEN timezone('America/New_York', u.created_at)                  
			WHEN u.locale = 'pt_BR' THEN timezone('America/Sao_Paulo', u.created_at)                        
			WHEN u.locale = 'es_CO' THEN timezone('America/Bogota', u.created_at)                   
			WHEN u.locale = 'en_PH' THEN timezone('Asia/Manila', u.created_at)               
			WHEN u.locale = 'es_AR' THEN timezone('America/Argentina/Buenos_Aires', u.created_at)        
			WHEN u.locale = 'en_MY' THEN timezone('Asia/Kuala_Lumpur', u.created_at)          
			WHEN u.locale = 'zh_TW' THEN timezone('Asia/Taipei', u.created_at)			     
			WHEN u.locale = 'es_MX' THEN timezone('America/Mexico_City', u.created_at)		             
			WHEN u.locale = 'en_SG' THEN timezone('Asia/Singapore', u.created_at)	     
			WHEN u.locale = 'es_CL' THEN timezone('America/Santiago', u.created_at)  		             
			WHEN u.locale = 'es_UY' THEN timezone('America/Montevideo', u.created_at)           
			WHEN u.locale = 'en_IN' THEN timezone('Asia/Kolkata', u.created_at)			     
			WHEN u.locale = 'en_PK' THEN timezone('Asia/Karachi', u.created_at) END) AS  created_at,
		(CASE WHEN u.locale = 'en_US'  THEN 'United States' 		       
			WHEN u.locale = 'pt_BR' THEN 'Brasil'
			WHEN u.locale = 'es_CO' THEN 'Colombia'
			WHEN u.locale = 'en_PH' THEN 'Philippines'
			WHEN u.locale = 'es_AR' THEN 'Argentina + Uruguay'
			WHEN u.locale = 'en_MY' THEN 'Malaysia + Singapore'
			WHEN u.locale = 'zh_TW' THEN 'Taiwan'
			WHEN u.locale = 'es_MX' THEN 'Mexico'
			WHEN u.locale = 'en_SG' THEN 'Malaysia + Singapore'
			WHEN u.locale = 'es_CL' THEN 'Chile'  
			WHEN u.locale = 'es_UY' THEN 'Argentina + Uruguay'		       
			WHEN u.locale = 'en_IN' THEN 'India' 
			WHEN u.locale = 'en_PK' THEN 'Pakistan' END) AS country, 
		(CASE WHEN u.locale = 'en_US'  THEN 'United States' 		       
			WHEN u.locale = 'pt_BR' THEN 'Brasil'
			WHEN u.locale = 'es_CO' THEN 'Colombia'
			WHEN u.locale = 'en_PH' THEN 'Philippines'
			WHEN u.locale = 'es_AR' THEN 'Argentina'
			WHEN u.locale = 'en_MY' THEN 'Malaysia'
			WHEN u.locale = 'zh_TW' THEN 'Taiwan'
			WHEN u.locale = 'es_MX' THEN 'Mexico'
			WHEN u.locale = 'en_SG' THEN 'Singapore'
			WHEN u.locale = 'es_CL' THEN 'Chile'  
			WHEN u.locale = 'es_UY' THEN 'Uruguay'		       
			WHEN u.locale = 'en_IN' THEN 'India' 
			WHEN u.locale = 'en_PK' THEN 'Pakistan' END) AS locale, 
		u.affiliate,
		u.mobile_phone_number AS phone_number,
		u.email,
		'http://www.facebook.com/' || u.facebook_id AS facebook_profile,
		u.pref_chat,
		u.pref_food,
		u.pref_music,
		u.pref_pets,
		u.pref_smoking,
		u.facebook_friends,
		(CASE WHEN u.share_phone = '1'THEN 1 END) AS share_phone, 		 
		(CASE WHEN u.share_email = '1'THEN 1 END) AS share_email, 		 	
		(CASE WHEN u.share_facebook = '1'THEN 1 END) AS share_facebook, 		 	
		(CASE WHEN (u.share_nothing = '1') OR  ((u.share_phone = '0' OR u.share_phone IS NULL) AND 
												(u.share_email = '0' OR u.share_email IS NULL) AND 
												(u.share_facebook = '0' OR u.share_facebook IS NULL)) THEN 1 END) AS share_nothing,
		(CASE WHEN u.mobile_phone_verified_at IS NOT NULL THEN 1 ELSE 0 END) AS phone_verified,
		(CASE WHEN u.email_verified_at IS NOT NULL THEN 1 ELSE 0 END) AS email_verified

	FROM
		users u
	WHERE
		u.is_active = '1' AND u.blocked IS FALSE) tprinc
	LEFT JOIN (SELECT
				u.id AS id,
				MIN(date(timezone(departure_city.timezone, tc.created_at))) AS first_offer,
				MAX(date(timezone(departure_city.timezone, tc.created_at))) AS last_offer,
				count(tl.id) AS total_offer
			FROM trip_configurations tc
				JOIN users u ON u.id = tc.user_id
				JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
				JOIN trip_legs AS tl ON tl.trip_leg_configuration_id = tlc.id
				JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
			    JOIN states AS departure_state ON departure_state.id = departure_city.state_id
			    JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
			WHERE
				tlc.departure_index = 0 
				AND tlc.destination_index = -1
			GROUP BY 
				u.id) toff ON tprinc.id = toff.id
	LEFT JOIN (SELECT
				u.id AS id,
				MIN(date(timezone(departure_city.timezone, tl.departure_datetime))) AS first_driver,
				MAX(date(timezone(departure_city.timezone, tl.departure_datetime))) AS last_driver
			FROM trip_configurations tc
				JOIN users u ON u.id = tc.user_id
				JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
				JOIN trip_legs AS tl ON tl.trip_leg_configuration_id = tlc.id
				JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
			    JOIN states AS departure_state ON departure_state.id = departure_city.state_id
			    JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
			WHERE
				tlc.departure_index = 0 
				AND tlc.destination_index = -1
				AND tl.is_canceled IS FALSE
				AND tl.departure_datetime < now()
			GROUP BY 
				u.id) tdriver ON tprinc.id = tdriver.id
	LEFT JOIN (SELECT
				u.id AS id,
				MIN(date(timezone(departure_city.timezone, tl.departure_datetime))) AS first_driver_with_booking,
				MAX(date(timezone(departure_city.timezone, tl.departure_datetime))) AS last_driver_with_booking
			FROM trip_configurations tc
				JOIN users u ON u.id = tc.user_id
				JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
				JOIN trip_legs AS tl ON tl.trip_leg_configuration_id = tlc.id
				JOIN trip_bookings tb ON tl.id = tb.trip_leg_id
				JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
			    JOIN states AS departure_state ON departure_state.id = departure_city.state_id
			    JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
			WHERE
				tlc.departure_index = 0 
				AND tlc.destination_index = -1
				AND tl.departure_datetime < now()
				AND tl.is_canceled IS FALSE
				AND tb.canceled_at IS NULL
				AND tb.rejected_at IS NULL
			GROUP BY 
				u.id) toffbook ON tprinc.id = toffbook.id
	LEFT JOIN (SELECT
				u.id,
				MIN(date(timezone(departure_city.timezone, tb.created_at))) AS first_booking,
				MAX(date(timezone(departure_city.timezone, tb.created_at))) AS last_booking,
				count(tb.id) AS total_booking
			FROM
				trip_bookings tb
				JOIN trip_legs tl ON tl.id = tb.trip_leg_id
				JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id   
				JOIN users u ON u.id = tb.user_id 
				JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
			    JOIN states AS departure_state ON departure_state.id = departure_city.state_id
			    JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
			GROUP BY
				u.id) tbook ON tprinc.id = tbook.id
	LEFT JOIN (SELECT
				u.id,
				MIN(date(timezone(departure_city.timezone, tl.departure_datetime))) AS first_pax,
				MAX(date(timezone(departure_city.timezone, tl.departure_datetime))) AS last_pax
			FROM
				trip_bookings tb
				JOIN trip_legs tl ON tl.id = tb.trip_leg_id
				JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id   
				JOIN users u ON u.id = tb.user_id 
				JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
			    JOIN states AS departure_state ON departure_state.id = departure_city.state_id
			    JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
			WHERE
				tl.departure_datetime < now()
				AND tl.is_canceled IS FALSE
				AND tb.canceled_at IS NULL
				AND tb.rejected_at IS NULL
			GROUP BY
				u.id) tpax ON tprinc.id = tpax.id
	LEFT JOIN (SELECT
				u.id AS id,
				b.name AS badges,
				count(ur.rating) AS ratings,
				sum(ur.rating) AS total_rating,
				(CASE WHEN ur.comment IS NOT NULL THEN 1 ELSE 0 END) AS have_comments 
			FROM
				users u
				LEFT JOIN user_badges ub ON ub.user_id = u.id
				LEFT JOIN badges b ON ub.badge_id = b.id
				LEFT JOIN user_ratings ur ON ur.rated_user_id = u.id
			GROUP BY
				u.id,
				b.name,
				have_comments) tsec ON tsec.id = tprinc.id
	LEFT JOIN(SELECT
				u.id AS id,
				tand.android AS android,
				tios.ios AS ios
			FROM
				users u
			LEFT JOIN (SELECT
						DISTINCT u.id AS id,
						(CASE WHEN ud.device_platform = 'android' THEN 1 END) AS android 
					FROM
						users u
						JOIN user_devices ud ON ud.user_id = u.id
					WHERE
						ud.device_platform = 'android'
					ORDER BY u.id) tand ON tand.id = u.id
			LEFT JOIN (SELECT
						DISTINCT u.id AS id,
						(CASE WHEN ud.device_platform = 'ios' THEN 1 END) AS ios
					FROM
						users u
						JOIN user_devices ud ON ud.user_id = u.id
					WHERE
						ud.device_platform = 'ios'
					ORDER BY u.id) tios ON tios.id = u.id
			ORDER BY	
				u.id) top ON top.id = tprinc.id
				
		

