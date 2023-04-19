CREATE TABLE integers (
    ts TIMESTAMP,
    TIME INDEX(ts)
);

INSERT INTO integers VALUES (1), (2), (3), (4), (5);

SELECT * FROM integers;

-- Test insert with long string constant
CREATE TABLE IF NOT EXISTS presentations (
    presentation_date TIMESTAMP,
    author VARCHAR NOT NULL,
    title STRING NOT NULL,
    bio VARCHAR,
    abstract VARCHAR,
    zoom_link VARCHAR,
    TIME INDEX(presentation_date)
);

insert into presentations values (1, 'Patrick Damme', 'Analytical Query Processing Based on Continuous Compression of Intermediates', NULL, 'Modern in-memory column-stores are widely accepted as the adequate database architecture for the efficient processing of complex analytical queries over large relational data volumes. These systems keep their entire data in main memory and typically employ lightweight compression to address the bottleneck between main memory and CPU. Numerous lightweight compression algorithms have been proposed in the past years, but none of them is suitable in all cases. While lightweight compression is already well established for base data, the efficient representation of intermediate results generated during query processing has attracted insufficient attention so far, although in in-memory systems, accessing intermeFdiates is as expensive as accessing base data. Thus, our vision is a continuous use of lightweight compression for all intermediates in a query execution plan, whereby a suitable compression algorithm should be selected for each intermediate. In this talk, I will provide an overview of our research in the context of this vision, including an experimental survey of lightweight compression algorithms, our compression-enabled processing model, and our compression-aware query optimization strategies.', 'https://zoom.us/j/7845983526');

DROP TABLE integers;

DROP TABLE presentations;
