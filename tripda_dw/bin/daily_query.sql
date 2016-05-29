SELECT 	 cu.report_date AS report_date,  cu.country AS Country,  sum(tb1.pax_transported) AS Pax_Transported_Sort,  sum(tb.trip_realized) AS Trip_Realized, 	 sum(tb.pax_transported) AS Pax_Transported, 	sum(tbs.rejection) AS Driver_Rejection, 	 sum(tbs.trip_cancelation) AS Driver_Cancelation, 	sum(tbs.pax_cancelation) AS Pax_Cancelation, 	 sum(tt.trip_offered) AS Trip_Offered, 	 sum(tb.distance_Km) AS Seats_Km, 	 sum(co.new_trip_offered) AS New_Trip_Offered,  	  sum(cb.new_bookings) AS New_Bookings, 	 sum(cu.new_users) AS New_Users, 	 sum(td.drivers) AS New_Drivers, 	  sum(tp.passangers) AS New_Passangers, 	  sum(task.seats_km) AS ASK, 	 trunc(sum(tb.distance_Km)/sum(tb.pax_transported),2) AS Average_Distance, 	trunc(sum(tb.pax_transported)/sum(tb.trip_realized),2) AS Avarage_Pax_per_Trip   FROM  	(SELECT  		 date(tl.departure_datetime) AS trip_date, departure_country.name AS country,	count(tl.departure_datetime) AS trip_offered 		 FROM  trip_configurations tc 		 JOIN users u ON u.id = tc.user_id 		 JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id 		 JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id AND tl.is_canceled = '0') 		 JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                  		 JOIN states AS departure_state ON departure_state.id = departure_city.state_id                               		 JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     	 WHERE 		 u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea')  		AND tlc.departure_index = 0  		AND tlc.destination_index = -1 GROUP BY  		date(tl.departure_datetime), 		departure_country.name ORDER BY date(tl.departure_datetime), departure_country.name) tt  FULL JOIN (SELECT 		date(tl.departure_datetime) AS trip_date, 		departure_country.name AS country, 		sum ( tb.number_of_seats) AS pax_transported, 		sum(CASE WHEN tb.rejected_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END) AS rejection, 		sum(CASE WHEN tl.is_canceled = '1' THEN tb.number_of_seats ELSE 0 END) AS trip_cancelation, 		sum(CASE WHEN tb.canceled_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END) AS pax_cancelation 	FROM 		trip_bookings tb 		JOIN trip_legs tl ON tl.id = tb.trip_leg_id 		JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id    		JOIN users u ON u.id = tb.user_id 		JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id      		JOIN states AS departure_state ON departure_state.id = departure_city.state_id      		JOIN countries AS departure_country ON departure_country.id = departure_state.country_id      	WHERE 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) 	GROUP BY 		date(tl.departure_datetime), 		departure_country.name 	ORDER BY 		date(tl.departure_datetime) 	) tbs ON 	tt.trip_date=tbs.trip_date AND 	tt.country=tbs.country    FULL JOIN 	(SELECT 		date(tl.departure_datetime) AS trip_date, 		departure_country.name AS country, 		count ( DISTINCT(driver.first_name || tl.departure_datetime) ) AS trip_realized, 		sum ( tb.number_of_seats) AS pax_transported, 		sum (tlc.distance*tb.number_of_seats) AS distance_Km 	FROM 		trip_bookings tb 		JOIN trip_legs tl ON tl.id = tb.trip_leg_id 		JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id    		JOIN users u ON u.id = tb.user_id 	INNER JOIN trip_configurations AS tc ON tc.id = tlc.trip_configuration_id    INNER JOIN users AS driver ON driver.id = tc.user_id	JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id      		JOIN states AS departure_state ON departure_state.id = departure_city.state_id      		JOIN countries AS departure_country ON departure_country.id = departure_state.country_id      	WHERE 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) AND  		tb.rejected_at IS NULL  AND   		tl.is_canceled = '0' AND  		tb.canceled_at IS NULL 	GROUP BY 		date(tl.departure_datetime), 		departure_country.name 	ORDER BY 		date(tl.departure_datetime) 	) tb ON 	tb.trip_date=tt.trip_date AND 	tb.country=tt.country    FULL JOIN 	(SELECT  		departure_country.name AS country, 	sum ( tb.number_of_seats) AS pax_transported	 	FROM 		trip_bookings tb 		JOIN trip_legs tl ON tl.id = tb.trip_leg_id 		JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id    		JOIN users u ON u.id = tb.user_id 	INNER JOIN trip_configurations AS tc ON tc.id = tlc.trip_configuration_id    INNER JOIN users AS driver ON driver.id = tc.user_id	JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id      		JOIN states AS departure_state ON departure_state.id = departure_city.state_id      		JOIN countries AS departure_country ON departure_country.id = departure_state.country_id      	WHERE 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) AND  		tb.rejected_at IS NULL  AND   		tl.is_canceled = '0' AND  		tb.canceled_at IS NULL AND date(tl.departure_datetime) = date(now())-1	GROUP BY		departure_country.name	) tb1 ON 	tb1.country=tt.country   FULL JOIN 	(SELECT d.day AS report_date, (CASE WHEN d.loc = 'en_US' THEN 'United States' 		      WHEN d.loc = 'pt_BR' THEN 'Brasil'  		      WHEN d.loc = 'es_CO' THEN 'Colombia' 		      WHEN d.loc = 'en_PH' THEN 'Philippines' 		      WHEN d.loc = 'es_AR' THEN 'Argentina' 		      WHEN d.loc = 'en_MY' THEN 'Malaysia' 		      WHEN d.loc = 'zh_TW' THEN 'Taiwan' 		      WHEN d.loc = 'es_MX' THEN 'Mexico' 		      WHEN d.loc = 'en_SG' THEN 'Singapore' 		      WHEN d.loc = 'es_CL' THEN 'Chile' 		      WHEN d.loc = 'en_IN' THEN 'India'  WHEN d.loc = 'es_UY' THEN 'Uruguay' WHEN d.loc = 'en_PK' THEN 'Pakistan' END) AS country , count(users.id) AS new_users FROM  (SELECT DISTINCT date(d) AS DAY,  users.locale AS loc FROM users CROSS JOIN generate_series( 	  CURRENT_DATE - 30,  	  CURRENT_DATE - 1,  	  '1 day' 	) d) d  LEFT JOIN users ON date(users.created_at) = d.day AND users.locale = d.loc WHERE d.loc = 'en_US' OR d.loc = 'pt_BR' OR d.loc = 'es_CO' OR d.loc = 'en_PH' OR d.loc = 'es_AR' OR  d.loc = 'en_MY' OR d.loc = 'zh_TW' OR d.loc = 'es_MX' OR  d.loc = 'en_SG' OR  d.loc = 'es_CL' OR  d.loc = 'en_IN' OR  d.loc = 'es_UY' OR  d.loc = 'en_PK' AND  users.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426') AND users.is_active = '1'  GROUP BY DAY, d.loc ORDER BY DAY) cu ON cu.country = tt.country AND    cu.report_date = tt.trip_date  FULL JOIN 	(SELECT  		count( DISTINCT tc.id) AS new_trip_offered, 		departure_country.name AS country, 		date(tc.created_at) AS report_date 	FROM trip_configurations tc 		JOIN users u ON u.id = tc.user_id 		INNER JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id 		INNER JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id AND tl.is_canceled = '0') 		INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                  		INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                               		INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     	WHERE 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea')   	GROUP BY  		date(tc.created_at), 		departure_country.name) co 		 ON 	co.report_date = cu.report_date AND 	co.country = cu.country  FULL JOIN  	(SELECT  		count(*) AS new_bookings, 		departure_country.name AS country, 		date(tb.created_at) AS report_date 	FROM  		trip_bookings tb 		LEFT JOIN trip_legs AS tl ON (tl.id = tb.trip_leg_id  AND tl.is_canceled = '0')  		LEFT JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id   		INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                  		INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                               		INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     		JOIN users u ON u.id = tb.user_id 	WHERE 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426')   	GROUP BY  		date(tb.created_at), 		departure_country.name) cb       ON  	co.report_date = cb.report_date AND 	co.country = cb.country   LEFT JOIN 	(SELECT 	tdaux.driver_start, 	tdaux.country, 	count(tdaux.id) AS drivers 	FROM 		(SELECT  		DISTINCT u.guid AS id, 		(CASE WHEN u.locale = 'en_US' THEN 'United States' 				 WHEN u.locale = 'pt_BR' THEN 'Brasil'  				  WHEN u.locale = 'es_CO' THEN 'Colombia' 				  WHEN u.locale = 'en_PH' THEN 'Philippines' 				  WHEN u.locale = 'es_AR' THEN 'Argentina' 				  WHEN u.locale = 'en_MY' THEN 'Malaysia' 				  WHEN u.locale = 'zh_TW' THEN 'Taiwan' 				  WHEN u.locale = 'es_MX' THEN 'Mexico' 				 WHEN u.locale = 'en_SG' THEN 'Singapore' 				  WHEN u.locale = 'es_CL' THEN 'Chile' 				  WHEN u.locale = 'en_IN' THEN 'India' WHEN u.locale = 'es_UY' THEN 'Uruguay' WHEN u.locale = 'en_PK' THEN 'Pakistan' END) AS country, 			MIN(date(tc.created_at)) AS driver_start 		FROM 			trip_configurations AS tc  			RIGHT JOIN users AS u ON (tc.user_id = u.id) 		GROUP BY 			u.guid,country) tdaux 	GROUP BY 		tdaux.driver_start, 		tdaux.country) td ON 	td.country = tt.country AND 	td.driver_start = tt.trip_date  LEFT JOIN 	(SELECT 	tpaux.passanger_start, 	tpaux.country, 	count(tpaux.id) AS passangers 	FROM 		(SELECT  		DISTINCT u.guid AS id, 		(CASE WHEN u.locale = 'en_US' THEN 'United States' 				 WHEN u.locale = 'pt_BR' THEN 'Brasil'  				  WHEN u.locale = 'es_CO' THEN 'Colombia' 				  WHEN u.locale = 'en_PH' THEN 'Philippines' 				  WHEN u.locale = 'es_AR' THEN 'Argentina' 				  WHEN u.locale = 'en_MY' THEN 'Malaysia' 				  WHEN u.locale = 'zh_TW' THEN 'Taiwan' 				  WHEN u.locale = 'es_MX' THEN 'Mexico' 				 WHEN u.locale = 'en_SG' THEN 'Singapore' 				  WHEN u.locale = 'es_CL' THEN 'Chile' 				  WHEN u.locale = 'en_IN' THEN 'India'  WHEN u.locale = 'es_UY' THEN 'Uruguay' WHEN u.locale = 'en_PK' THEN 'Pakistan' END) AS country, 			MIN(date(tb.created_at)) AS passanger_start 		FROM 			trip_bookings AS tb  			RIGHT JOIN users AS u ON (tb.user_id = u.id) 		GROUP BY 			u.guid,country) tpaux 	GROUP BY 		tpaux.passanger_start, 		tpaux.country) tp ON 	tp.country = tt.country AND 	tp.passanger_start = tt.trip_date  FULL JOIN 	 (SELECT 		 	taux.report_date, 		 	taux.country, 		 	sum (taux.seats_km) AS seats_km 	 FROM 		 	(SELECT  			 		date(tl.departure_datetime) AS report_date, 			 		departure_country.name AS country, 			 		tlc.departure_address AS departure_address, 			 		tlc.destination_address AS destination_address, 			 		tl.departure_datetime AS departure_datetime, 			 		tl.created_at AS created_at, 			 		tc.seats_offered*tc.distance AS seats_km 		 	FROM  		trip_configurations tc 			 		JOIN users u ON u.id = tc.user_id 			 		INNER JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id 			 		INNER JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id AND tl.is_canceled = '0') 			 		INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                  			 		INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                               			 		INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     		 	WHERE 			 		u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea')   		 		AND 			tlc.departure_index = 0  	AND tlc.destination_index = -1 	) taux 	 	GROUP BY 		taux.report_date, 		taux.country ORDER BY taux.report_date, 		taux.country) task ON 	task.country = tt.country AND 	task.report_date = tt.trip_date   WHERE 	date(cu.report_date) = date(now())-1 OR 	date(cu.report_date) = date(now())-8 OR 	date(cu.report_date) = date(now())-15 OR 	date(cu.report_date) = date(now())-22 GROUP BY 	cu.report_date, 	cu.country ORDER BY 	cu.country , cu.report_date DESC