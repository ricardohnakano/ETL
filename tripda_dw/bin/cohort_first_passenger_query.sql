SELECT 
    bookings.passenger_id as user_id, 
    bookings.country as country,
    YEAR(bookings.booking_created_at) as year,
    MONTH(bookings.booking_created_at) as month,
    bookings.seats_booked*bookings.trip_distance as distance    
FROM
    bookings
    JOIN (SELECT
            bookings.passenger_id as id,
            min(bookings.booking_created_at) as create_date
        FROM   
            bookings
        WHERE
            bookings.booking_canceled = 0 AND
            bookings.booking_rejected = 0 AND
            bookings.trip_canceled = 0
        GROUP BY
            bookings.passenger_id) t1 ON t1.id = bookings.passenger_id and t1.create_date = bookings.booking_created_at
