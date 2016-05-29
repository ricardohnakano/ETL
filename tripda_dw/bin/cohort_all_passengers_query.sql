SELECT
    bookings.passenger_id AS user_id,
    YEAR(bookings.booking_created_at) AS YEAR,
    MONTH(bookings.booking_created_at) AS MONTH,
    sum(bookings.seats_booked*bookings.trip_distance) AS distance,
    COUNT(*) AS count
FROM   
    bookings
    LEFT JOIN (SELECT
                    bookings.passenger_id AS id,
                    bookings.passenger_id AS table_key,
                    min(bookings.booking_created_at) AS create_date
                FROM   
                    bookings
                WHERE
                    bookings.booking_canceled = 0 AND
                    bookings.booking_rejected = 0 AND
                    bookings.trip_canceled = 0
                GROUP BY
                    bookings.passenger_id) t1 ON t1.id = bookings.passenger_id AND t1.create_date = bookings.booking_created_at
WHERE
    bookings.booking_canceled = 0 AND
    bookings.booking_rejected = 0 AND
    bookings.trip_canceled = 0
GROUP BY
    bookings.passenger_id,
    YEAR(bookings.booking_created_at),
    MONTH(bookings.booking_created_at)

