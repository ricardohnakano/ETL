SELECT
    t3.id as user_id,
    MONTH(t3.create_date) as month,
    YEAR(t3.create_date) as year,
    t3.country as country,
    t3.distance as distance
FROM
    (SELECT
        trips.driver_id as id,
        trips.trip_created_at as create_date,
        sum(trips.seats_booked) - sum(trips.seats_canceled) - sum(trips.seats_rejected) AS seats,  
        (sum(trips.seats_booked) - sum(trips.seats_canceled) - sum(trips.seats_rejected))*trips.trip_distance AS distance,
        trips.country as country
    FROM
        trips
    WHERE   
        trips.trip_canceled = 0
    GROUP BY
        trips.driver_id,
        trips.trip_created_at,
        trips.country) t3
    JOIN (SELECT
            t1.id as id,
            min(t1.create_date) as create_date
        FROM 
            (SELECT
                trips.driver_id as id,
                trips.trip_created_at as create_date,
                sum(trips.seats_booked) - sum(trips.seats_canceled) - sum(trips.seats_rejected) AS seats
            FROM
                trips
            WHERE   
                trips.trip_canceled = 0
            GROUP BY
                trips.driver_id,
                trips.trip_created_at) t1
        WHERE
            t1.seats > 0
        GROUP BY
            t1.id) t2 ON t2.id = t3.id and t2.create_date = t3.create_date