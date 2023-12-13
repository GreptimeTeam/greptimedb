-- SQLNESS REPLACE branch:\s+.+ branch: BRANCH
-- SQLNESS REPLACE commit:\s+.+ commit: COMMIT
-- SQLNESS REPLACE commit+\s+short:\s+.+ commit short: COMMIT SHORT
-- SQLNESS REPLACE dirty:\s+.+ dirty: DIRTY
-- SQLNESS REPLACE version:\s+.+ version: VERSION
-- SQLNESS REPLACE [\s\-]+
SELECT build();
