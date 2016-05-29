SELECT
    EXTRACT(YEAR FROM tprinc.report_date) AS report_year,
    EXTRACT(week FROM tprinc.report_date) AS reference_number,
    (CASE WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 44 THEN '26.Oct - 01.Nov'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 45 THEN '02.Nov - 08.Nov'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 46 THEN '09.Nov - 15.Nov'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 47 THEN '16.Nov - 22.Nov'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 48 THEN '23.Nov - 29.Nov'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 49 THEN '30.Dec - 06.Dec'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 50 THEN '07.Dec - 13.Dec'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 51 THEN '14.Dec - 20.Dec'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2015 AND EXTRACT(week FROM tprinc.report_date) = 52 THEN '21.Dec - 27.Dec'
            WHEN (EXTRACT(YEAR FROM tprinc.report_date) = 2015 OR EXTRACT(YEAR FROM tprinc.report_date) = 2016) AND EXTRACT(week FROM tprinc.report_date) = 53 THEN '28.Dec - 03.Jan'
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 1 THEN  '04.Jan - 10.Jan' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 2 THEN  '11.Jan - 17.Jan' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 3 THEN  '18.Jan - 24.Jan' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 4 THEN  '25.Jan - 31.Jan' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 5 THEN  '01.Feb - 07.Feb' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 6 THEN  '08.Feb - 14.Feb' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 7 THEN  '15.Feb - 21.Feb' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 8 THEN  '22.Feb - 28.Feb' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 9 THEN  '29.Feb - 06.Mar' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 10 THEN  '07.Mar - 13.Mar' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 11 THEN  '14.Mar - 20.Mar' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 12 THEN  '21.Mar - 27.Mar' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 13 THEN  '28.Mar - 03.Apr' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 14 THEN  '04.Apr - 10.Apr' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 15 THEN  '11.Apr - 17.Apr' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 16 THEN  '18.Apr - 24.Apr' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 17 THEN  '25.Apr - 01.May' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 18 THEN  '02.May - 08.May' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 19 THEN  '09.May - 15.May' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 20 THEN  '16.May - 22.May' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 21 THEN  '23.May - 29.May' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 22 THEN  '30.May - 05.Jun' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 23 THEN  '06.Jun - 12.Jun' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 24 THEN  '13.Jun - 19.Jun' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 25 THEN  '20.Jun - 26.Jun' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 26 THEN  '27.Jun - 03.Jul' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 27 THEN  '04.Jul - 10.Jul' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 28 THEN  '11.Jul - 17.Jul' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 29 THEN  '18.Jul - 24.Jul' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 30 THEN  '25.Jul - 31.Jul' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 31 THEN  '01.Aug - 07.Aug' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 32 THEN  '08.Aug - 14.Aug' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 33 THEN  '15.Aug - 21.Aug' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 34 THEN  '22.Aug - 28.Aug' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 35 THEN  '29.Aug - 04.Sep' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 36 THEN  '05.Sep - 11.Sep' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 37 THEN  '12.Sep - 18.Sep' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 38 THEN  '19.Sep - 25.Sep' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 39 THEN  '26.Sep - 02.Oct' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 40 THEN  '03.Oct - 09.Oct' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 41 THEN  '10.Oct - 16.Oct' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 42 THEN  '17.Oct - 23.Oct' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 43 THEN  '24.Oct - 30.Oct' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 44 THEN  '31.Oct - 06.Nov' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 45 THEN  '07.Nov - 13.Nov' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 46 THEN  '14.Nov - 20.Nov' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 47 THEN  '21.Nov - 27.Nov' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 48 THEN  '28.Nov - 04.Dec' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 49 THEN  '05.Dec - 11.Dec' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 50 THEN  '12.Dec - 18.Dec' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 51 THEN  '19.Dec - 25.Dec' 
            WHEN EXTRACT(YEAR FROM tprinc.report_date) = 2016 AND EXTRACT(week FROM tprinc.report_date) = 52 THEN  '26.Dec - 01.Jan' END) AS report_reference,
    tprinc.country AS Country,
    sum(tb.trip_realized) AS Trip_Realized,
    sum(tuniquetrips.unique_trips_realized) AS Unique_Trip_Realized,
    sum(tb.pax_transported) AS Pax_Transported,
    sum(tuniquepd.unique_pax_driver) AS Unique_Pax_Driver,
    sum(tbs.rejection) AS Driver_Rejection,
    sum(tbs.trip_cancelation) AS Driver_Cancelation,
    sum(tbs.pax_cancelation) AS Pax_Cancelation,
    sum(tt.trip_offered) AS Trip_Offered,
    sum(tb.distance_Km) AS Seats_Km,
    sum(co.new_trip_offered) AS New_Trip_Offered, 
    sum(cb.new_bookings) AS New_Bookings,
    sum(cu.new_users) AS New_Users,
    sum(td.drivers) AS New_Drivers,
    sum(tp.passangers) AS New_Passangers,
    sum(task.seats_km) AS ASK,
    trunc(sum(tb.distance_Km)/sum(tb.pax_transported),2) AS Average_Distance,
    trunc(sum(tb.pax_transported)/sum(tb.trip_realized),2) AS Avarage_Pax_per_Trip
FROM 
    (SELECT
        t1.country AS country,
        t2.report_date AS report_date 
    FROM
        (SELECT
            (CASE WHEN u.locale = 'en_US' THEN 'United States'
              WHEN u.locale = 'pt_BR' THEN 'Brasil' 
              WHEN u.locale = 'es_CO' THEN 'Colombia'
              WHEN u.locale = 'en_PH' THEN 'Philippines'
              WHEN u.locale = 'es_AR' THEN 'Argentina'
              WHEN u.locale = 'en_MY' THEN 'Malaysia'
              WHEN u.locale = 'zh_TW' THEN 'Taiwan'
              WHEN u.locale = 'es_MX' THEN 'Mexico'
              WHEN u.locale = 'en_SG' THEN 'Singapore'
              WHEN u.locale = 'es_CL' THEN 'Chile'
              WHEN u.locale = 'en_IN' THEN 'India' 
              WHEN u.locale = 'es_UY' THEN 'Uruguay' 
              WHEN u.locale = 'en_PK' THEN 'Pakistan'END) AS country
        FROM
            users u
        WHERE
            (u.locale = 'en_US' 
            OR u.locale = 'pt_BR' 
            OR u.locale = 'es_CO'
            OR u.locale = 'en_PH' 
            OR u.locale = 'es_AR' 
            OR u.locale = 'en_MY' 
            OR u.locale = 'zh_TW' 
            OR u.locale = 'es_MX' 
            OR u.locale = 'en_SG' 
            OR u.locale = 'es_CL' 
            OR u.locale = 'en_IN' 
            OR u.locale = 'es_UY' 
            OR u.locale = 'en_PK') 
        GROUP BY
            u.locale) t1
        CROSS JOIN 
            (SELECT
                date(u.created_at) AS report_date
            FROM
                users u
            GROUP BY
                date(u.created_at)) t2) tprinc 
FULL JOIN   
    (SELECT 
         date(tl.departure_datetime) AS trip_date, 
         departure_country.name AS country,  
         count(tl.departure_datetime) AS trip_offered
    FROM  
        trip_configurations tc
        JOIN users u ON u.id = tc.user_id
        JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
        JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id AND tl.is_canceled = '0')
        JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                 
        JOIN states AS departure_state ON departure_state.id = departure_city.state_id                              
        JOIN countries AS departure_country ON departure_country.id = departure_state.country_id    
    WHERE
        u.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3','30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea') 
        AND u.blocked IS FALSE
        AND tlc.departure_index = 0 
        AND tlc.destination_index = -1 GROUP BY 
        date(tl.departure_datetime),
        departure_country.name 
    ORDER BY 
        date(tl.departure_datetime), 
        departure_country.name) tt ON tt.trip_date = tprinc.report_date AND tt.country = tprinc.country  
FULL JOIN 
    (SELECT
        date(tl.departure_datetime) AS trip_date,
        departure_country.name AS country,
        sum ( tb.number_of_seats) AS pax_transported,
        sum(CASE WHEN tb.rejected_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END) AS rejection,
        sum(CASE WHEN tl.is_canceled = '1' THEN tb.number_of_seats ELSE 0 END) AS trip_cancelation,
        sum(CASE WHEN tb.canceled_at IS NOT NULL THEN tb.number_of_seats ELSE 0 END) AS pax_cancelation
    FROM
        trip_bookings tb
        JOIN trip_legs tl ON tl.id = tb.trip_leg_id
        JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id   
        JOIN trip_configurations tc ON tc.id = tlc.trip_configuration_id
        JOIN users u ON u.id = tb.user_id
        JOIN users driver ON driver.id = tc.user_id
        JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id     
        JOIN states AS departure_state ON departure_state.id = departure_city.state_id     
        JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     
    WHERE
        driver.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3') AND
        driver.blocked IS FALSE AND
        u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) AND
        u.blocked IS FALSE
    GROUP BY
        date(tl.departure_datetime),
        departure_country.name
    ORDER BY
        date(tl.departure_datetime)
        ) tbs ON tprinc.report_date=tbs.trip_date AND tprinc.country=tbs.country   
FULL JOIN
    (SELECT
        date(tl.departure_datetime) AS trip_date,
        departure_country.name AS country,
        count ( DISTINCT(CAST(tc.id AS TEXT) || tl.departure_datetime) ) AS trip_realized,
        sum ( tb.number_of_seats) AS pax_transported,
        sum (tlc.distance*tb.number_of_seats) AS distance_Km
    FROM
        trip_bookings tb
        JOIN trip_legs tl ON tl.id = tb.trip_leg_id
        JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id   
        JOIN users u ON u.id = tb.user_id
        INNER JOIN trip_configurations AS tc ON tc.id = tlc.trip_configuration_id   
        INNER JOIN users AS driver ON driver.id = tc.user_id        
        JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id     
        JOIN states AS departure_state ON departure_state.id = departure_city.state_id     
        JOIN countries AS departure_country ON departure_country.id = departure_state.country_id     
    WHERE
        driver.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3') AND
        driver.blocked IS FALSE AND
        u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) AND 
        u.blocked IS FALSE AND
        tb.rejected_at IS NULL  AND  
        tl.is_canceled = '0' AND 
        tb.canceled_at IS NULL
    GROUP BY
        date(tl.departure_datetime),
        departure_country.name
    ORDER BY
        date(tl.departure_datetime) ) tb ON tb.trip_date=tprinc.report_date AND tb.country=tprinc.country   
FULL JOIN
    (SELECT
        count(*) AS new_users,
        (CASE WHEN u.locale = 'en_US' THEN 'United States'
              WHEN u.locale = 'pt_BR' THEN 'Brasil' 
              WHEN u.locale = 'es_CO' THEN 'Colombia'
              WHEN u.locale = 'en_PH' THEN 'Philippines'
              WHEN u.locale = 'es_AR' THEN 'Argentina'
              WHEN u.locale = 'en_MY' THEN 'Malaysia'
              WHEN u.locale = 'zh_TW' THEN 'Taiwan'
              WHEN u.locale = 'es_MX' THEN 'Mexico'
              WHEN u.locale = 'en_SG' THEN 'Singapore'
              WHEN u.locale = 'es_CL' THEN 'Chile'
              WHEN u.locale = 'en_IN' THEN 'India' WHEN u.locale = 'es_UY' THEN 'Uruguay' WHEN u.locale = 'en_PK' THEN 'Pakistan' END) AS country,
        date(u.created_at) AS report_date
    FROM
        users u
    WHERE
        u.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3','30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426') 
        AND u.is_active = '1' 
        AND u.blocked IS FALSE
    GROUP BY
        date(u.created_at),
        u.locale) cu ON cu.country = tprinc.country AND    cu.report_date = tprinc.report_date
FULL JOIN
    (SELECT 
        count( DISTINCT tc.id) AS new_trip_offered,
        departure_country.name AS country,
        date(tc.created_at) AS report_date
    FROM 
        trip_configurations tc
        JOIN users u ON u.id = tc.user_id
        INNER JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
        INNER JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id)
        INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                 
        INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                              
        INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id    
    WHERE
        u.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3','30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea')  
        AND u.blocked IS FALSE
    GROUP BY 
        date(tc.created_at),
        departure_country.name) co ON co.report_date = tprinc.report_date AND co.country = tprinc.country
FULL JOIN 
    (SELECT 
        count(*) AS new_bookings,
        departure_country.name AS country,
        date(tb.created_at) AS report_date
    FROM 
        trip_bookings tb
        LEFT JOIN trip_legs AS tl ON (tl.id = tb.trip_leg_id) 
        LEFT JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id  
        INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id     
        JOIN trip_configurations tc ON tc.id = tlc.trip_configuration_id                            
        INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                              
        INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id    
        JOIN users u ON u.id = tb.user_id
        INNER JOIN users AS driver ON driver.id = tc.user_id
    WHERE
        driver.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3') AND
        driver.blocked IS FALSE AND
        u.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426')  AND
        u.blocked IS FALSE
    GROUP BY 
        date(tb.created_at),
        departure_country.name) cb ON tprinc.report_date = cb.report_date AND tprinc.country = cb.country 
FULL JOIN
    (SELECT
        tdaux.driver_start,
        tdaux.country,
        count(tdaux.id) AS drivers
    FROM
        (SELECT 
            DISTINCT u.guid AS id,
            departure_country.name AS country,
            MIN(date(tc.created_at)) AS driver_start
        FROM
            trip_configurations AS tc
            JOIN cities AS departure_city ON departure_city.id = tc.departure_city_id
            JOIN states AS departure_state ON departure_state.id = departure_city.state_id
            JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
            RIGHT JOIN users AS u ON (tc.user_id = u.id)
        WHERE 
        u.is_active = '1'
        AND u.blocked IS FALSE
        GROUP BY
            u.guid,country) tdaux
    GROUP BY
        tdaux.driver_start,
        tdaux.country) td ON td.country = tprinc.country AND td.driver_start = tprinc.report_date 
LEFT JOIN
    (SELECT
        tpaux.passanger_start,
        tpaux.country,
        count(tpaux.id) AS passangers
    FROM
        (SELECT 
        DISTINCT u.guid AS id,
        departure_country.name AS country,
        MIN(date(tb.created_at)) AS passanger_start
        FROM
            trip_bookings AS tb 
            JOIN trip_legs AS tl ON (tl.id = tb.trip_leg_id)
            JOIN trip_leg_configurations AS tlc ON tlc.id = tl.trip_leg_configuration_id
            JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id 
            JOIN states AS departure_state ON departure_state.id = departure_city.state_id 
            JOIN countries AS departure_country ON departure_country.id = departure_state.country_id 
            RIGHT JOIN users AS u ON (tb.user_id = u.id) 
        WHERE 
            u.is_active = '1'
            AND u.blocked IS FALSE  
        GROUP BY
            u.guid,country) tpaux
    GROUP BY
        tpaux.passanger_start,
        tpaux.country) tp ON tp.country = tprinc.country AND tp.passanger_start = tprinc.report_date
FULL JOIN
    (SELECT
        taux.report_date,
        taux.country,
        sum (taux.seats_km) AS seats_km
    FROM
        (SELECT 
            date(tl.departure_datetime) AS report_date,
            departure_country.name AS country,
            tlc.departure_address AS departure_address,
            tlc.destination_address AS destination_address,
            tl.departure_datetime AS departure_datetime,
            tl.created_at AS created_at,
            tc.seats_offered*tc.distance AS seats_km
        FROM 
            trip_configurations tc
            JOIN users u ON u.id = tc.user_id
            INNER JOIN trip_leg_configurations AS tlc ON tlc.trip_configuration_id = tc.id
            INNER JOIN trip_legs AS tl ON (tl.trip_leg_configuration_id = tlc.id AND tl.is_canceled = '0')
            INNER JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id                                 
            INNER JOIN states AS departure_state ON departure_state.id = departure_city.state_id                              
            INNER JOIN countries AS departure_country ON departure_country.id = departure_state.country_id    
        WHERE
            u.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3','30932f00-9862-4039-aea3-20d052e7dc53', '73535830-4412-4304-bc76-091361452f2b', '2737e141-525c-40ad-9031-d632e9fe6d07', 'f22d4ee9-5960-4019-ac20-d0ce52ccaeff', '5669b34c-67d3-4470-b93a-a00756a973b8', 'ae338e24-be9e-4c67-86be-e90c3fbfc57c', '8fcc9eb1-602f-4464-be56-8ad1028c2c04', '177ccacd-a363-40e5-a23c-e2bcc550d224', '34cb2d85-81b5-4fc6-a596-1adcd8f47a2a', '5cb8ad99-c476-4ba0-a2b0-a6560b7372b2', '6591fa5a-8fd2-4ae8-832d-c520fbe9862d', 'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a', 'e0acbd91-9229-4411-8020-0ae56d6d9552', '2d933580-ed88-4a81-ab4f-5cd12a997ff7', '47c5ab88-4aea-4ce7-8c2e-291db3ca17bb', 'f877ab95-d146-430d-a7d3-4ee188bc089e', 'f5d631b2-cdb0-4a6f-befb-d8001d2ad383', '1e9cc6bb-cd3f-4d09-8b17-daa982a317ea')  
            AND u.blocked IS FALSE
            AND tlc.departure_index = 0 
            AND tlc.destination_index = -1) taux
    GROUP BY
        taux.report_date,
        taux.country 
    ORDER BY 
        taux.report_date,
        taux.country) task ON task.country = tprinc.country AND task.report_date = tprinc.report_date  
FULL JOIN
    (SELECT  
        t1.country AS report_country,
        t1.report_date AS report_date,
        t1.country,
        count(DISTINCT t1.trip) AS unique_trips_realized
    FROM
        (SELECT
            tint.trip AS trip,
            min(tint.report_date) AS report_date,
            tint.country AS country
        FROM
            (SELECT
                array_to_string(ARRAY(SELECT unnest(array_append(array_agg(DISTINCT trip_bookings.user_id ORDER BY trip_bookings.user_id ASC),trip_configurations.user_id)) AS uid ORDER BY uid),',') AS trip,
                date(trip_legs.departure_datetime) AS report_date,
                departure_country.name AS country
            FROM 
                trip_bookings
                JOIN trip_legs ON trip_bookings.trip_leg_id = trip_legs.id
                JOIN trip_leg_configurations AS tlc ON tlc.id = trip_legs.trip_leg_configuration_id
                JOIN trip_configurations AS tc ON tc.id = tlc.trip_configuration_id
                JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
                JOIN states AS departure_state ON departure_state.id = departure_city.state_id
                JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
                JOIN trip_configurations ON trip_configurations.id = tlc.trip_configuration_id
                JOIN users AS passenger ON passenger.id = trip_bookings.user_id
                JOIN users AS driver ON driver.id = tc.user_id
            WHERE
                trip_bookings.canceled_at IS NULL
                AND trip_bookings.rejected_at IS NULL
                AND trip_legs.is_canceled = FALSE
                AND driver.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3') 
                AND driver.blocked IS FALSE
                AND passenger.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) 
                AND passenger.blocked IS FALSE
            GROUP BY 
                trip_bookings.trip_leg_id,
                trip_legs.departure_datetime,
                departure_country.name,
                trip_configurations.user_id ) tint
        GROUP BY
            tint.trip,
            tint.country) t1
        GROUP BY 
            t1.country,
            t1.report_date) tuniquetrips ON tuniquetrips.report_country = tprinc.country AND tuniquetrips.report_date = tprinc.report_date 
FULL JOIN
    (SELECT  
        t1.country AS report_country,
        t1.report_date AS report_date,
        t1.country,
        count(DISTINCT t1.trip) AS unique_pax_driver
    FROM
        (SELECT
            tint.trip AS trip,
            min(tint.report_date) AS report_date,
            tint.country AS country
        FROM
            (SELECT
                CAST(driver.id AS TEXT)|| ' - '||CAST(passenger.id AS TEXT) AS trip,
                date(trip_legs.departure_datetime) AS report_date,
                departure_country.name AS country
            FROM 
                trip_bookings
                JOIN trip_legs ON trip_bookings.trip_leg_id = trip_legs.id
                JOIN trip_leg_configurations AS tlc ON tlc.id = trip_legs.trip_leg_configuration_id
                JOIN trip_configurations AS tc ON tc.id = tlc.trip_configuration_id
                JOIN cities AS departure_city ON departure_city.id = tlc.departure_city_id
                JOIN states AS departure_state ON departure_state.id = departure_city.state_id
                JOIN countries AS departure_country ON departure_country.id = departure_state.country_id
                JOIN trip_configurations ON trip_configurations.id = tlc.trip_configuration_id
                JOIN users AS passenger ON passenger.id = trip_bookings.user_id
                JOIN users AS driver ON driver.id = tc.user_id
            WHERE
                trip_bookings.canceled_at IS NULL
                AND trip_bookings.rejected_at IS NULL
                AND trip_legs.is_canceled = FALSE
                AND driver.guid NOT IN ('5788b25a-fbb6-4ee3-b2e0-c6d239dada08','93a33b54-fd34-4575-ad72-3028b7e368b3') 
                AND driver.blocked IS FALSE
                AND passenger.guid NOT IN ('30932f00-9862-4039-aea3-20d052e7dc53','73535830-4412-4304-bc76-091361452f2b','30932f00-9862-4039-aea3-20d052e7dc53' ,'73535830-4412-4304-bc76-091361452f2b' ,'2737e141-525c-40ad-9031-d632e9fe6d07' ,'f22d4ee9-5960-4019-ac20-d0ce52ccaeff' ,'5669b34c-67d3-4470-b93a-a00756a973b8' ,'ae338e24-be9e-4c67-86be-e90c3fbfc57c' ,'8fcc9eb1-602f-4464-be56-8ad1028c2c04' ,'177ccacd-a363-40e5-a23c-e2bcc550d224' ,'34cb2d85-81b5-4fc6-a596-1adcd8f47a2a' ,'5cb8ad99-c476-4ba0-a2b0-a6560b7372b2' ,'6591fa5a-8fd2-4ae8-832d-c520fbe9862d' ,'e81a32a3-e2cb-4b3f-9f04-cc4db1ad678a' ,'e0acbd91-9229-4411-8020-0ae56d6d9552' ,'2d933580-ed88-4a81-ab4f-5cd12a997ff7' ,'47c5ab88-4aea-4ce7-8c2e-291db3ca17bb' ,'f877ab95-d146-430d-a7d3-4ee188bc089e' ,'f5d631b2-cdb0-4a6f-befb-d8001d2ad383' ,'e27888a4-d054-47c2-a359-210d2b4cff8d' ,'1e9cc6bb-cd3f-4d09-8b17-daa982a317ea' ,'78ba300f-26dc-408c-8de7-15ac499512de' ,'b58bbeaf-0f74-40b8-ae9e-9697d04f0841' ,'bea52e68-f4c3-4d46-a527-d68c59784426' ) 
                AND passenger.blocked IS FALSE
            GROUP BY 
                trip_bookings.trip_leg_id,
                trip_legs.departure_datetime,
                departure_country.name ,
                driver.id,
                passenger.id) tint
        GROUP BY
            tint.trip,
            tint.country) t1
        GROUP BY 
            t1.country,
            t1.report_date)  tuniquepd ON tuniquepd.report_country = tprinc.country AND tuniquepd.report_date = tprinc.report_date 
WHERE 
    (CASE WHEN (EXTRACT(YEAR FROM now())) = '2016'  AND (EXTRACT(week FROM now())) = '1' THEN
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '50') OR 
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '51') OR
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '52') OR
            ((EXTRACT(YEAR FROM tprinc.report_date) = '2015' OR EXTRACT(YEAR FROM tprinc.report_date) = '2016') AND EXTRACT(week FROM tprinc.report_date) = '53') 
        WHEN (EXTRACT(YEAR FROM now())) = '2016'  AND (EXTRACT(week FROM now())) = '2' THEN
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '51') OR 
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '52') OR 
            ((EXTRACT(YEAR FROM tprinc.report_date) = '2015' OR EXTRACT(YEAR FROM tprinc.report_date) = '2016') AND EXTRACT(week FROM tprinc.report_date) = '53') OR 
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '1')
        WHEN (EXTRACT(YEAR FROM now())) = '2016'  AND (EXTRACT(week FROM now())) = '3' THEN
            (EXTRACT(YEAR FROM tprinc.report_date) = '2015'  AND EXTRACT(week FROM tprinc.report_date) = '52') OR 
            ((EXTRACT(YEAR FROM tprinc.report_date) = '2015' OR EXTRACT(YEAR FROM tprinc.report_date) = '2016') AND EXTRACT(week FROM tprinc.report_date) = '53') OR
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '1') OR
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '2')
        WHEN (EXTRACT(YEAR FROM now())) = '2016'  AND (EXTRACT(week FROM now())) = '4' THEN
            ((EXTRACT(YEAR FROM tprinc.report_date) = '2015' OR EXTRACT(YEAR FROM tprinc.report_date) = '2016') AND EXTRACT(week FROM tprinc.report_date) = '53') OR 
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '1') OR
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '2') OR
            (EXTRACT(YEAR FROM tprinc.report_date) = '2016'  AND EXTRACT(week FROM tprinc.report_date) = '3')
        ELSE
            EXTRACT(YEAR FROM tprinc.report_date) = EXTRACT(YEAR FROM date(now()))  AND 
            (EXTRACT(week FROM tprinc.report_date) = EXTRACT(week FROM date(now())) -1 OR 
            EXTRACT(week FROM tprinc.report_date) = (EXTRACT(week FROM date(now())) -2) OR 
            EXTRACT(week FROM tprinc.report_date) = (EXTRACT(week FROM date(now())) -3) OR 
            EXTRACT(week FROM tprinc.report_date) = (EXTRACT(week FROM date(now())) -4)) END)
GROUP BY
    report_year,
    reference_number,
    report_reference,
    tprinc.country  
ORDER BY
    reference_number