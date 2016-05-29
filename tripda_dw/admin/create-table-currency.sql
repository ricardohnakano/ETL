CREATE TABLE `currency` (
  `id` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci  NOT NULL,
  `date` date DEFAULT NULL,
  `country` varchar(45) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci  NULL,
  `rate` decimal(6,6) DEFAULT NULL,
  PRIMARY KEY (`id`)
)
CHARACTER SET = utf8mb4
COLLATE = utf8mb4_unicode_ci
ENGINE = INNODB;