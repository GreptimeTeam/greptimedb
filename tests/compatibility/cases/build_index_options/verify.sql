ALTER TABLE legacy_build_index MODIFY COLUMN msg SET FULLTEXT INDEX;
ADMIN BUILD_INDEX('legacy_build_index');
SELECT msg FROM legacy_build_index WHERE MATCHES(msg, 'hello') ORDER BY ts;
ADMIN BUILD_INDEX('legacy_build_index');
SELECT COUNT(*) FROM legacy_build_index;
