-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
-- 
--   http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
-- 
-- Unnest tests from Apache DataFusion

-- Basic unnest tests
SELECT unnest([1,2,3]);

SELECT unnest(struct(1,2,3));

-- Table function is not supported for now
-- SELECT * FROM unnest([1,2,3]);

-- SELECT * FROM unnest(struct(1,2,3));

-- SELECT * FROM unnest(struct(1,2,3)), unnest([4,5,6]);

-- Multiple unnest levels
SELECT unnest([1,2,3]), unnest(unnest([[1,2,3]]));

SELECT unnest([1,2,3]) + unnest([1,2,3]), unnest([1,2,3]) + unnest([4,5]);

SELECT unnest(unnest([[1,2,3]])) + unnest(unnest([[1,2,3]])), 
       unnest(unnest([[1,2,3]])) + unnest([4,5]), 
       unnest([4,5]);

-- Unnest with expressions
SELECT unnest([1,2,3]) + 1, unnest(unnest([[1,2,3]]))* 2;


-- Complex subquery with unnest
-- table function is not supported for now
-- WITH t AS (
--   SELECT
--     left1,
--     width1,
--     min(column3) as min_height
--   FROM
--     unnest(ARRAY[1,2,3,4,5,6,7,8,9,10]) AS t(left1)
--     CROSS JOIN unnest(ARRAY[1,2,3,4,5,6,7,8,9,10]) AS t1(width1)
--   WHERE
--     left1 + width1 - 1 <= 10
--     AND column3 BETWEEN left1 AND left1 + width1 - 1
--   GROUP BY
--     left1, width1
-- )
-- SELECT
--   left1, width1, min_height, min_height * width1 as area
-- FROM t
-- WHERE min_height * width1 = (
--   SELECT max(min_height * width1) FROM t
-- );

