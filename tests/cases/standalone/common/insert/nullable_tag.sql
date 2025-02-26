CREATE TABLE `esT`(
  `eT` TIMESTAMP(3) TIME INDEX,
  `eAque` BOOLEAN,
  `DoLOruM` INT,
  `repudiAndae` STRING,
  `ULLaM` BOOLEAN,
  `COnSECTeTuR` SMALLINT DEFAULT -31852,
  `DOLOrIBUS` FLOAT NOT NULL,
  `QUiS` SMALLINT NULL,
  `consEquatuR` BOOLEAN NOT NULL,
  `vERO` BOOLEAN,
  PRIMARY KEY(`repudiAndae`, `ULLaM`, `DoLOruM`)
);

INSERT INTO `esT` (
  `consEquatuR`,
  `eAque`,
  `eT`,
  `repudiAndae`,
  `DOLOrIBUS`
)
VALUES
(
  false,
  false,
  '+234049-06-04 01:11:41.163+0000',
  'hello',
  0.97377783
),
(
  false,
  true,
  '-19578-12-20 11:45:59.875+0000',
  NULL,
  0.3535998
);

SELECT * FROM `esT` order by `eT` desc;

DROP TABLE `esT`;
