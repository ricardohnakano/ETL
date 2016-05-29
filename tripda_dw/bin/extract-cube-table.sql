SELECT 
	tprinc.id AS id,
	tprinc.reference_date AS reference_date,
	tprinc.report_country AS country,
	uc.new_users AS new_users,
	uo.new_driver AS new_driver,
	ub.new_passenger AS new_passenger,
	tc.new_trip_offered AS new_trip_offered,
	td.ask AS ask,
	td.trip_offered AS trip_offered,
	bc.new_booking AS new_booking,
	bd.trip_realized AS trip_realized,
	bd.pax_transported AS pax_transported,
	bd.seats_price*tprinc.rate AS seats_price,
	bd.seats_distance AS seats_distance,
	bd.booking_cancelation AS booking_cancelation,
	bd.trip_cancelation AS trip_cancelation,
	bd.booking_rejection AS booking_rejection
FROM   
	(SELECT
		c.id,
		c.date AS reference_date,
		c.country AS country,
		(CASE WHEN c.country = 'Argentina'  THEN 'Argentina + Uruguay'
				WHEN c.country = 'Uruguay' THEN 'Argentina + Uruguay'
				WHEN c.country = 'Malaysia' THEN 'Malaysia + Singapore'
				WHEN c.country = 'Singapore' THEN 'Malaysia + Singapore'
				ELSE c.country END) AS report_country,
		c.rate
	FROM
		currency c	
	) tprinc

	LEFT JOIN (SELECT
					u.country,
					DATE(u.created_at) AS created_at,
					COUNT(*) AS new_users
				FROM
					users u
				GROUP BY
					u.country,
					DATE(u.created_at)
				) uc ON uc.country = tprinc.report_country AND DATE(uc.created_at) = tprinc.reference_date

	LEFT JOIN (SELECT
					u.country,
					DATE(u.first_offer) AS first_offer,
					COUNT(*) AS new_driver
				FROM
					users u
				GROUP BY
					u.country,
					DATE(u.first_offer)
				) uo ON uo.country = tprinc.report_country AND uo.first_offer = tprinc.reference_date

	LEFT JOIN (SELECT
					u.country,
					DATE(u.first_booking) AS first_booking,
					COUNT(*) AS new_passenger
				FROM
					users u
				GROUP BY
					u.country,
					DATE(u.first_booking)
				) ub ON ub.country = tprinc.report_country AND ub.first_booking = tprinc.reference_date

	LEFT JOIN (SELECT
					t.departure_country AS country,
					DATE(t.trip_created_at) AS created_at,
					COUNT(*) AS new_trip_offered
				FROM
					trips t
				WHERE
					t.main_leg = 1
				GROUP BY
					t.departure_country,
					DATE(t.trip_created_at)
				) tc ON tc.country = tprinc.country AND DATE(tc.created_at) = tprinc.reference_date

	LEFT JOIN (SELECT
					t.departure_country AS country,
					DATE(t.trip_departure_datetime) AS departure_datetime,
					COUNT(*) AS trip_offered,
					sum(t.trip_distance*t.seats_offered) AS ask
				FROM
					trips t
				WHERE
					t.main_leg = 1
				GROUP BY
					t.departure_country,
					DATE(t.trip_departure_datetime)
				) td ON td.country = tprinc.country AND DATE(td.departure_datetime) = tprinc.reference_date

	LEFT JOIN (SELECT
					b.departure_country AS country,
					DATE(b.booking_created_at) AS created_at,
					COUNT(*) AS new_booking
				FROM
					bookings b
				GROUP BY
					b.departure_country,
					DATE(b.booking_created_at)
				) bc ON bc.country = tprinc.country AND DATE(bc.created_at) = tprinc.reference_date

	LEFT JOIN (SELECT
					b.departure_country AS country,
					DATE(b.trip_departure_datetime) AS trip_departure_datetime,
					COUNT(DISTINCT b.trip_id) AS trip_realized,
					sum(b.seats_booked) AS pax_transported,
					sum(b.seats_booked*b.price) AS seats_price,
					sum(b.seats_booked*b.trip_distance) AS seats_distance,
					sum(CASE WHEN b.booking_canceled = 1 THEN b.seats_booked ELSE 0 END) AS booking_cancelation,
					sum(CASE WHEN b.trip_canceled = 1 THEN b.seats_booked ELSE 0 END) AS trip_cancelation,
					sum(CASE WHEN b.booking_rejected = 1 THEN b.seats_booked ELSE 0 END) AS booking_rejection
				FROM 
					bookings b
				GROUP BY
					b.departure_country,
					DATE(b.trip_departure_datetime)
				) bd ON bd.country = tprinc.country AND DATE(bd.trip_departure_datetime) = tprinc.reference_date

GROUP BY
	tprinc.reference_date,
	tprinc.report_country