ALTER TABLE users
ADD `locale` VARCHAR( 45 ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NULL

ALTER TABLE users
ADD `first_driver_with_booking` DATE NULL

ALTER TABLE users
ADD `last_driver_with_booking` DATE NULL