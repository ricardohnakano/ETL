CREATE TABLE `cube` (
  `id` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci  NOT NULL,
  `reference_date` date DEFAULT NULL,
  `country` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci  NULL,
  `new_users` int(6) DEFAULT NULL,
  `new_driver` int(6) DEFAULT NULL,
  `new_passenger` int(6) DEFAULT NULL,
  `new_trip_offered` int(6) DEFAULT NULL,
  `ask` int(10) DEFAULT NULL,
  `trip_offered` int(6) DEFAULT NULL,
  `new_booking` int(6) DEFAULT NULL,
  `trip_realized` int(6) DEFAULT NULL,
  `pax_transported` int(6) DEFAULT NULL,
  `seats_price` int(10) DEFAULT NULL,
  `seats_distance` int(10) DEFAULT NULL,
  `booking_cancelation` int(6) DEFAULT NULL,
  `trip_cancelation` int(6) DEFAULT NULL,
  `booking_rejection` int(6) DEFAULT NULL,
  PRIMARY KEY (`id`)
) 
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci
ENGINE = INNODB;