SELECT
    t3.id AS user_id,
    MONTH(t3.create_date) AS MONTH,
    YEAR(t3.create_date) AS YEAR,
    sum(t3.distance) AS distance,
    COUNT(*) AS COUNT
FROM
    (SELECT
        trips.driver_id AS id,
        trips.trip_created_at AS create_date,
        sum(trips.seats_booked) - sum(trips.seats_canceled) - sum(trips.seats_rejected) AS seats, 
        (sum(trips.seats_booked) - sum(trips.seats_canceled) - sum(trips.seats_rejected) )*trips.trip_distance AS distance
    FROM
        trips
    WHERE   
        trips.trip_canceled = 0
    GROUP BY
        trips.driver_id,
        trips.trip_created_at) t3
    LEFT JOIN (SELECT
                    t1.id AS id,
                    t1.id AS table_key,
                    min(t1.create_date) AS create_date
                FROM 
                    (SELECT
                        trips.driver_id AS id,
                        trips.trip_created_at AS create_date,
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
                    t1.id) t2 ON t2.id = t3.id AND t2.create_date = t3.create_date
WHERE
    t3.seats > 0
GROUP BY
    user_id,
    MONTH,
    YEAR