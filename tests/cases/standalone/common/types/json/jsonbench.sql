CREATE TABLE bluesky (
    `data`  JSON2,
    time_us TimestampMicrosecond TIME INDEX
) WITH ('append_mode' = 'true', 'sst_format' = 'flat');

INSERT INTO bluesky (time_us, data)
VALUES (1732206349000167,
        '{"did":"did:plc:yj3sjq3blzpynh27cumnp5ks","time_us":1732206349000167,"kind":"commit","commit":{"rev":"3lbhtytnn2k2f","operation":"create","collection":"app.bsky.feed.post","rkey":"3lbhtyteurk2y","record":{"$type":"app.bsky.feed.post","createdAt":"2024-11-21T16:09:27.095Z","langs":["en"],"reply":{"parent":{"cid":"bafyreibfglofvqou2yiqvwzk4rcgkhhxrbunyemshdjledgwymimqkg24e","uri":"at://did:plc:6tr6tuzlx2db3rduzr2d6r24/app.bsky.feed.post/3lbhqo2rtys2z"},"root":{"cid":"bafyreibfglofvqou2yiqvwzk4rcgkhhxrbunyemshdjledgwymimqkg24e","uri":"at://did:plc:6tr6tuzlx2db3rduzr2d6r24/app.bsky.feed.post/3lbhqo2rtys2z"}},"text":"aaaaah.  LIght shines in a corner of WTF...."},"cid":"bafyreidblutgvj75o4q4akzyyejedjj6l3it6hgqwee6jpwv2wqph5fsgm"}}');

INSERT INTO bluesky (time_us, data)
VALUES (1732206349000644,
        '{"did":"did:plc:3i4xf2v4wcnyktgv6satke64","time_us":1732206349000644,"kind":"commit","commit":{"rev":"3lbhuvzds6d2a","operation":"create","collection":"app.bsky.feed.like","rkey":"3lbhuvzdked2a","record":{"$type":"app.bsky.feed.like","createdAt":"2024-11-21T16:25:46.221Z","subject":{"cid":"bafyreidjvrcmckkm765mct5fph36x7kupkfo35rjklbf2k76xkzwyiauge","uri":"at://did:plc:azrv4rcbws6kmcga4fsbphg2/app.bsky.feed.post/3lbgjdpbiec2l"}},"cid":"bafyreia5l5vrkh5oj4cjyhcqby2dprhyvcyofo2q5562tijlae2pzih23m"}}');

ADMIN flush_table('bluesky');

INSERT INTO bluesky (time_us, data)
VALUES (1732206349001108,
        '{"did":"did:plc:gccfnqqizz4urhchsaie6jft","time_us":1732206349001108,"kind":"commit","commit":{"rev":"3lbhuvze3gi2u","operation":"create","collection":"app.bsky.graph.follow","rkey":"3lbhuvzdtmi2u","record":{"$type":"app.bsky.graph.follow","createdAt":"2024-11-21T16:27:40.923Z","subject":"did:plc:r7cdh4sgzqbfdc6wcdxxti7c"},"cid":"bafyreiew2p6cgirfaj45qoenm4fgumib7xoloclrap3jgkz5es7g7kby3i"}}');

INSERT INTO bluesky (time_us, data)
VALUES (1732206349001372,
        '{"did":"did:plc:msxqf3twq7abtdw7dbfskphk","time_us":1732206349001372,"kind":"commit","commit":{"rev":"3lbhueija5p22","operation":"create","collection":"app.bsky.feed.like","rkey":"3lbhueiizcx22","record":{"$type":"app.bsky.feed.like","createdAt":"2024-11-21T16:15:58.232Z","subject":{"cid":"bafyreiavpshyqzrlo5m7fqodjhs6jevweqnif4phasiwimv4a7mnsqi2fe","uri":"at://did:plc:fusulxqc52zbrc75fi6xrcof/app.bsky.feed.post/3lbhskq5zn22f"}},"cid":"bafyreidjix4dauj2afjlbzmhj3a7gwftcevvmmy6edww6vrjdbst26rkby"}}');

ADMIN flush_table('bluesky');

INSERT INTO bluesky (time_us, data)
VALUES (1732206349001905,
        '{"did":"did:plc:l5o3qjrmfztir54cpwlv2eme","time_us":1732206349001905,"kind":"commit","commit":{"rev":"3lbhtytohxc2o","operation":"create","collection":"app.bsky.feed.post","rkey":"3lbhtytjqzk2q","record":{"$type":"app.bsky.feed.post","createdAt":"2024-11-21T16:09:27.254Z","langs":["en"],"reply":{"parent":{"cid":"bafyreih35fe2jj3gchmgk4amold4l6sfxd2sby5wrg3jrws5fkdypxrbg4","uri":"at://did:plc:6wx2gg5yqgvmlu35r6y3bk6d/app.bsky.feed.post/3lbhtj2eb4s2o"},"root":{"cid":"bafyreifipyt3vctd4ptuoicvio7rbr5xvjv4afwuggnd2prnmn55mu6luu","uri":"at://did:plc:474ldquxwzrlcvjhhbbk2wte/app.bsky.feed.post/3lbhdzrynik27"}},"text":"okay i take mine back because I hadn’t heard this one yet^^"},"cid":"bafyreigzdsdne3z2xxcakgisieyj7y47hj6eg7lj6v4q25ah5q2qotu5ku"}}');

SELECT count(*) FROM bluesky;

-- Query 1:
SELECT data.commit.collection AS event,
       count() AS count
FROM bluesky
GROUP BY event
ORDER BY count DESC, event ASC;

-- Query 2:
SELECT data.commit.collection AS event,
       count() AS count,
       count(DISTINCT data.did) AS users
FROM bluesky
WHERE data.kind = 'commit' AND data.commit.operation = 'create'
GROUP BY event
ORDER BY count DESC, event ASC;

-- Query 3:
SELECT data.commit.collection AS event,
       date_part('hour', to_timestamp_micros(arrow_cast(data.time_us, 'Int64'))) as hour_of_day,
       count() AS count
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection in ('app.bsky.feed.post', 'app.bsky.feed.repost', 'app.bsky.feed.like')
GROUP BY event, hour_of_day
ORDER BY hour_of_day, event;

-- Query 4:
SELECT data.did::String as user_id,
       min(to_timestamp_micros(arrow_cast(data.time_us, 'Int64'))) AS first_post_ts
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY first_post_ts ASC, user_id DESC
LIMIT 3;

-- Query 5:
SELECT data.did::String as user_id,
       date_part(
           'epoch',
           max(to_timestamp_micros(arrow_cast(data.time_us, 'Int64'))) -
             min(to_timestamp_micros(arrow_cast(data.time_us, 'Int64')))
       ) AS activity_span
FROM bluesky
WHERE data.kind = 'commit'
  AND data.commit.operation = 'create'
  AND data.commit.collection = 'app.bsky.feed.post'
GROUP BY user_id
ORDER BY activity_span DESC, user_id DESC
LIMIT 3;

-- SQLNESS REPLACE (peers.*) REDACTED
EXPLAIN
SELECT date_part('hour', to_timestamp_micros(arrow_cast(data.time_us, 'Int64'))) as hour_of_day
FROM bluesky;
