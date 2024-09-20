CREATE TABLE jsons (j JSON, t timestamp time index);

--Insert valid json strings--
INSERT INTO jsons VALUES('[null]', 0),
('[true]', 1),
('[false]', 2),
('[0]', 3),
('["foo"]', 4),
('[]', 5),
('{}', 6),
('[0,1]', 7),
('{"foo":"bar"}', 8),
('{"a":null,"foo":"bar"}', 9),
('[-1]', 10),
('{"entities": {
    "description": {
        "urls": [
            {
                "url": "http://t.co/QMLJeFmfMT",
                "expanded_url": "http://www.pixiv.net/member.php?id=4776",
                "display_url": "pixiv.net/member.php?id=…",
                "indices": [
                    58,
                    80
                ]
            },
            {
                "url": "http://t.co/LU8T7vmU3h",
                "expanded_url": "http://ask.fm/KATANA77",
                "display_url": "ask.fm/KATANA77",
                "indices": [
                    95,
                    117
                ]
            }
        ]
    }
}}', 11);

INSERT INTO jsons VALUES(parse_json('[null]'), 12),
(parse_json('[true]'), 13),
(parse_json('[false]'), 14),
(parse_json('[0]'), 15),
(parse_json('["foo"]'), 16),
(parse_json('[]'), 17),
(parse_json('{}'), 18),
(parse_json('[0,1]'), 19),
(parse_json('{"foo":"bar"}'), 20),
(parse_json('{"a":null,"foo":"bar"}'), 21),
(parse_json('[-1]'), 22),
(parse_json('[-2147483648]'), 23),
(parse_json('{"entities": {
            "description": {
                "urls": [
                    {
                        "url": "http://t.co/QMLJeFmfMT",
                        "expanded_url": "http://www.pixiv.net/member.php?id=4776",
                        "display_url": "pixiv.net/member.php?id=…",
                        "indices": [
                            58,
                            80
                        ]
                    },
                    {
                        "url": "http://t.co/LU8T7vmU3h",
                        "expanded_url": "http://ask.fm/KATANA77",
                        "display_url": "ask.fm/KATANA77",
                        "indices": [
                            95,
                            117
                        ]
                    }
                ]
            }
        }}'), 24);

SELECT json_to_string(j), t FROM jsons;

--Insert invalid json strings--
DELETE FROM jsons;

INSERT INTO jsons VALUES(parse_json('{"a":1, "b":2, "c":3'), 4);

INSERT INTO jsons VALUES(parse_json('Morning my friends, have a nice day :)'), 5);

SELECT json_to_string(j), t FROM jsons;

CREATE TABLE json_empty (j JSON, t timestamp time index);

INSERT INTO json_empty VALUES(NULL, 2);

SELECT json_to_string(j), t FROM json_empty;

drop table jsons;

drop table json_empty;
