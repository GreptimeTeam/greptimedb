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
}}', 11),
('{"a":"abc\u2028tom"}', 12);

INSERT INTO jsons VALUES(parse_json('[null]'), 1000),
(parse_json('[true]'), 1001),
(parse_json('[false]'), 1002),
(parse_json('[0]'), 1003),
(parse_json('["foo"]'), 1004),
(parse_json('[]'), 1005),
(parse_json('{}'), 1006),
(parse_json('[0,1]'), 1007),
(parse_json('{"foo":"bar"}'), 1008),
(parse_json('{"a":null,"foo":"bar"}'), 1009),
(parse_json('[-1]'), 1010),
(parse_json('[-2147483648]'), 1011),
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
        }}'), 1012),
(parse_json('{"a":"abc\u2028tom"}'), 1013);

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
