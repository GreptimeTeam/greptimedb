-- Migrated from DuckDB test: test/sql/join/ multi-way join tests  
-- Tests complex multi-table join scenarios

CREATE TABLE users_multi(user_id INTEGER, username VARCHAR, email VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE posts_multi(post_id INTEGER, user_id INTEGER, title VARCHAR, content TEXT, created_date DATE, ts TIMESTAMP TIME INDEX);

CREATE TABLE comments_multi(comment_id INTEGER, post_id INTEGER, user_id INTEGER, comment_text VARCHAR, ts TIMESTAMP TIME INDEX);

CREATE TABLE likes_multi(like_id INTEGER, post_id INTEGER, user_id INTEGER, ts TIMESTAMP TIME INDEX);

INSERT INTO users_multi VALUES 
(1, 'alice', 'alice@example.com', 1000), (2, 'bob', 'bob@example.com', 2000),
(3, 'charlie', 'charlie@example.com', 3000), (4, 'diana', 'diana@example.com', 4000);

INSERT INTO posts_multi VALUES 
(1, 1, 'First Post', 'Hello World', '2023-01-01', 1000),
(2, 2, 'Tech Talk', 'About databases', '2023-01-02', 2000),
(3, 1, 'Update', 'Progress report', '2023-01-03', 3000),
(4, 3, 'Discussion', 'Open topic', '2023-01-04', 4000);

INSERT INTO comments_multi VALUES 
(1, 1, 2, 'Great post!', 1000), (2, 1, 3, 'Thanks for sharing', 2000),
(3, 2, 1, 'Very informative', 3000), (4, 3, 4, 'Keep it up', 4000),
(5, 4, 2, 'Interesting discussion', 5000);

INSERT INTO likes_multi VALUES 
(1, 1, 2, 1000), (2, 1, 3, 2000), (3, 1, 4, 3000),
(4, 2, 1, 4000), (5, 2, 3, 5000), (6, 3, 2, 6000);

-- Four-way join
SELECT 
  u.username as author,
  p.title,
  c.comment_text,
  commenter.username as commenter
FROM users_multi u
INNER JOIN posts_multi p ON u.user_id = p.user_id
INNER JOIN comments_multi c ON p.post_id = c.post_id
INNER JOIN users_multi commenter ON c.user_id = commenter.user_id
ORDER BY p.post_id, c.comment_id;

-- Multi-way join with aggregation
SELECT 
  u.username,
  COUNT(DISTINCT p.post_id) as posts_created,
  COUNT(DISTINCT c.comment_id) as comments_made,
  COUNT(DISTINCT l.like_id) as likes_given
FROM users_multi u
LEFT JOIN posts_multi p ON u.user_id = p.user_id
LEFT JOIN comments_multi c ON u.user_id = c.user_id  
LEFT JOIN likes_multi l ON u.user_id = l.user_id
GROUP BY u.user_id, u.username
ORDER BY posts_created DESC, comments_made DESC;

-- Post engagement analysis
SELECT 
  p.title,
  u.username as author,
  COUNT(DISTINCT c.comment_id) as comment_count,
  COUNT(DISTINCT l.like_id) as like_count,
  COUNT(DISTINCT c.user_id) as unique_commenters
FROM posts_multi p
INNER JOIN users_multi u ON p.user_id = u.user_id
LEFT JOIN comments_multi c ON p.post_id = c.post_id
LEFT JOIN likes_multi l ON p.post_id = l.post_id
GROUP BY p.post_id, p.title, u.username
ORDER BY like_count DESC, comment_count DESC;

-- Complex multi-join with conditions
SELECT 
  author.username as post_author,
  p.title,
  commenter.username as commenter,
  liker.username as liker
FROM posts_multi p
INNER JOIN users_multi author ON p.user_id = author.user_id
INNER JOIN comments_multi c ON p.post_id = c.post_id
INNER JOIN users_multi commenter ON c.user_id = commenter.user_id
INNER JOIN likes_multi l ON p.post_id = l.post_id
INNER JOIN users_multi liker ON l.user_id = liker.user_id
WHERE author.user_id != commenter.user_id 
  AND author.user_id != liker.user_id
ORDER BY p.post_id, commenter.user_id, liker.user_id;

-- Multi-join with subqueries  
SELECT 
  u.username,
  popular_posts.title,
  popular_posts.engagement_score
FROM users_multi u
INNER JOIN (
  SELECT 
    p.post_id, p.user_id, p.title,
    COUNT(DISTINCT l.like_id) + COUNT(DISTINCT c.comment_id) as engagement_score
  FROM posts_multi p
  LEFT JOIN likes_multi l ON p.post_id = l.post_id
  LEFT JOIN comments_multi c ON p.post_id = c.post_id
  GROUP BY p.post_id, p.user_id, p.title
  HAVING COUNT(DISTINCT l.like_id) + COUNT(DISTINCT c.comment_id) > 2
) popular_posts ON u.user_id = popular_posts.user_id
ORDER BY popular_posts.engagement_score DESC;

DROP TABLE users_multi;

DROP TABLE posts_multi;

DROP TABLE comments_multi;

DROP TABLE likes_multi;