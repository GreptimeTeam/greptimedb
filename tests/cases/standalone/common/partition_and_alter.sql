CREATE TABLE `molestiAe` (
                             `sImiLiQUE` FLOAT NOT NULL,
                             `amEt` TIMESTAMP(6) TIME INDEX,
                             `EXpLICaBo` DOUBLE,
                             PRIMARY KEY (`sImiLiQUE`)
) PARTITION ON COLUMNS (`sImiLiQUE`) (
  `sImiLiQUE` < -1,
  `sImiLiQUE` >= -1 AND `sImiLiQUE` < -0,
  `sImiLiQUE` >= 0
);

INSERT INTO `molestiAe` VALUES
                            (-2, 0, 0),
                            (-0.9, 0, 0),
                            (1, 0, 0);

SELECT COUNT(*) from `molestiAe`;

ALTER TABLE `molestiAe` SET 'ttl' = '1d';

DROP TABLE `molestiAe`;
