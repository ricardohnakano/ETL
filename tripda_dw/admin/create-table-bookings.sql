CREATE TABLE `bookings` ( 
    `id` INT( 11 ) NOT NULL, 
    `related_trip_id` INT( 11 ) NULL, 
    `trip_id` VARCHAR( 45 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `trip_created_at` DATETIME NULL, 
    `trip_departure_datetime` DATETIME NULL, 
    `departure_city` VARCHAR( 255 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `destination_city` VARCHAR( 255 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `departure_state` VARCHAR( 255 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `destination_state`VARCHAR( 255 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `departure_country` VARCHAR( 45 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `destination_country` VARCHAR( 45 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `country` VARCHAR( 45 ) CHARACTER SET utf8 COLLATE utf8_general_ci NULL, 
    `trip_distance` INT( 5 ) NULL, 
    `trip_canceled` INT( 1 ) NULL, 
    `driver_id` INT( 11 ) NULL, 
    `passenger_id` INT( 11 ) NULL, 
    `seats_booked` INT( 1 ) NULL, 
    `booking_created_at` DATETIME NULL, 
    `is_approved` INT( 1 ) NULL, 
    `booking_canceled` INT( 1 ) NULL, 
    `booking_rejected` INT( 1 ) NULL, 
    `price` INT( 6 ) NULL, 
    `ratings` INT( 11 ) NULL,
     PRIMARY KEY ( `id` )
 )
CHARACTER SET = utf8
COLLATE = utf8_general_ci
ENGINE = INNODB;