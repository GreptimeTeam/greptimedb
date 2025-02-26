-- SQLNESS REPLACE branch:\s+.+ branch: BRANCH
-- SQLNESS REPLACE commit:\s+.+ commit: COMMIT
-- SQLNESS REPLACE commit_short:\s+.+ commit_short: COMMIT_SHORT
-- SQLNESS REPLACE clean:\s+.+ clean: CLEAN
-- SQLNESS REPLACE version:\s+.+ version: VERSION
-- SQLNESS REPLACE [\s\-]+
SELECT build();

-- SQLNESS REPLACE (\d+\.\d+\.\d+) VERSION
-- SQLNESS REPLACE [\s\-]+
SELECT version();
