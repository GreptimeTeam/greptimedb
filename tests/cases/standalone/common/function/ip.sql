-- Create a table for IPv4 address testing
CREATE TABLE ip_v4_data (
    `id` INT,
    `time` TIMESTAMP DEFAULT 0,
    ip_addr STRING,
    ip_numeric UINT32,
    subnet_mask UINT8,
    cidr_range STRING,
    PRIMARY KEY(`id`),
    TIME INDEX(`time`)
);

-- Create a table for IPv6 address testing
CREATE TABLE ip_v6_data (
    `id` INT,
    `time` TIMESTAMP DEFAULT 0,
    ip_addr STRING,
    ip_hex STRING,
    subnet_mask UINT8,
    cidr_range STRING,
    PRIMARY KEY(`id`),
    TIME INDEX(`time`)
);

-- Create a table for network traffic analysis
CREATE TABLE network_traffic (
    `id` INT,
    `time` TIMESTAMP DEFAULT 0,
    source_ip STRING,
    dest_ip STRING,
    bytes_sent UINT64, 
    PRIMARY KEY(`id`),
    TIME INDEX(`time`)
);

-- Insert IPv4 test data
INSERT INTO ip_v4_data (`id`, ip_addr, ip_numeric, subnet_mask, cidr_range) VALUES
(1, '192.168.1.1', 3232235777, 24, '192.168.1.0/24'),
(2, '10.0.0.1', 167772161, 8, '10.0.0.0/8'),
(3, '172.16.0.1', 2886729729, 12, '172.16.0.0/12'),
(4, '127.0.0.1', 2130706433, 8, '127.0.0.0/8'),
(5, '8.8.8.8', 134744072, 32, '8.8.8.8/32'),
(6, '192.168.0.1', 3232235521, 16, '192.168.0.0/16'),
(7, '255.255.255.255', 4294967295, 32, '255.255.255.255/32'),
(8, '0.0.0.0', 0, 0, '0.0.0.0/0');

-- Insert IPv6 test data
INSERT INTO ip_v6_data (`id`, ip_addr, ip_hex, subnet_mask, cidr_range) VALUES
(1, '2001:db8::1', '20010db8000000000000000000000001', 32, '2001:db8::/32'),
(2, '::1', '00000000000000000000000000000001', 128, '::1/128'),
(3, 'fe80::1234', 'fe800000000000000000000000001234', 10, 'fe80::/10'),
(4, '::ffff:192.168.0.1', '00000000000000000000ffffc0a80001', 96, '::ffff:192.168.0.0/96'),
(5, '2001:db8:1::1', '20010db8000100000000000000000001', 48, '2001:db8:1::/48'),
(6, '2001:0:0:0:0:0:0:1', '20010000000000000000000000000001', 64, '2001::/64');

-- Insert network traffic data
INSERT INTO network_traffic (`id`, source_ip, dest_ip, bytes_sent) VALUES
(1, '192.168.1.5', '8.8.8.8', 1024),
(2, '10.0.0.15', '192.168.1.1', 2048),
(3, '192.168.1.1', '10.0.0.15', 4096),
(4, '172.16.0.5', '172.16.0.1', 8192),
(5, '2001:db8::1', '2001:db8::2', 16384),
(6, '2001:db8:1::5', '2001:db8:2::1', 32768),
(7, 'fe80::1234', 'fe80::5678', 65536),
(8, '::1', '::1', 131072);

-- Test IPv4 string/number conversion functions
SELECT 
    `id`, 
    ip_addr, 
    ip_numeric,
    ipv4_string_to_num(ip_addr) AS computed_numeric,
    ipv4_num_to_string(ip_numeric) AS computed_addr
FROM ip_v4_data;

-- Test IPv4 CIDR functions
-- SQLNESS SORT_RESULT 3 1
SELECT 
    `id`,
    ip_addr,
    subnet_mask,
    ipv4_to_cidr(ip_addr) AS auto_cidr,
    ipv4_to_cidr(ip_addr, subnet_mask) AS specified_cidr,
    cidr_range AS expected_cidr
FROM ip_v4_data;

-- Test IPv4 range checks
-- SQLNESS SORT_RESULT 3 1
SELECT 
    t.`id`,
    t.source_ip,
    t.dest_ip,
    t.bytes_sent,
    d.cidr_range,
    ipv4_in_range(t.source_ip, d.cidr_range) AS source_in_range,
    ipv4_in_range(t.dest_ip, d.cidr_range) AS dest_in_range
FROM network_traffic t
JOIN ip_v4_data d ON ipv4_in_range(t.source_ip, d.cidr_range) OR ipv4_in_range(t.dest_ip, d.cidr_range)
-- Only get IPv4 records
WHERE t.source_ip NOT LIKE '%:%';

-- Test IPv6 string/hex conversion functions
-- SQLNESS SORT_RESULT 3 1
SELECT 
    `id`, 
    ip_addr, 
    ip_hex,
    ipv6_num_to_string(ip_hex) AS computed_addr
FROM ip_v6_data;

-- Test IPv6 CIDR functions
-- SQLNESS SORT_RESULT 3 1
SELECT 
    `id`,
    ip_addr,
    subnet_mask,
    ipv6_to_cidr(ip_addr) AS auto_cidr,
    ipv6_to_cidr(ip_addr, subnet_mask) AS specified_cidr,
    cidr_range AS expected_cidr
FROM ip_v6_data;

-- Test IPv6 range checks
-- SQLNESS SORT_RESULT 3 1
SELECT 
    t.`id`,
    t.source_ip,
    t.dest_ip,
    t.bytes_sent,
    d.cidr_range,
    ipv6_in_range(t.source_ip, d.cidr_range) AS source_in_range,
    ipv6_in_range(t.dest_ip, d.cidr_range) AS dest_in_range
FROM network_traffic t
JOIN ip_v6_data d ON ipv6_in_range(t.source_ip, d.cidr_range) OR ipv6_in_range(t.dest_ip, d.cidr_range)
-- Only get IPv6 records
WHERE t.source_ip LIKE '%:%';

-- Combined IPv4/IPv6 example - Security analysis
-- Find all traffic from the same network to specific IPs
-- SQLNESS SORT_RESULT 3 1
SELECT 
    source_ip,
    dest_ip,
    bytes_sent,
    CASE 
        WHEN source_ip LIKE '%:%' THEN 
            ipv6_to_cidr(source_ip, arrow_cast(64, 'UInt8'))
        ELSE 
            ipv4_to_cidr(source_ip, arrow_cast(24, 'UInt8'))
    END AS source_network,
    CASE
        WHEN dest_ip LIKE '%:%' THEN
            'IPv6'
        ELSE
            'IPv4'
    END AS dest_type
FROM network_traffic
ORDER BY bytes_sent DESC;

-- Subnet analysis - IPv4
-- SQLNESS SORT_RESULT 3 1
SELECT 
    ipv4_to_cidr(source_ip, arrow_cast(24,'UInt8')) AS subnet,
    COUNT(*) AS device_count,
    SUM(bytes_sent) AS total_bytes
FROM network_traffic
WHERE source_ip NOT LIKE '%:%'
GROUP BY ipv4_to_cidr(source_ip, arrow_cast(24,'UInt8'))
ORDER BY total_bytes DESC;

-- Subnet analysis - IPv6
-- SQLNESS SORT_RESULT 3 1
SELECT 
    ipv6_to_cidr(source_ip, arrow_cast(48,'UInt8')) AS subnet,
    COUNT(*) AS device_count,
    SUM(bytes_sent) AS total_bytes
FROM network_traffic
WHERE source_ip LIKE '%:%'
GROUP BY ipv6_to_cidr(source_ip, arrow_cast(48,'UInt8'))
ORDER BY total_bytes DESC;

drop table ip_v4_data;

drop table ip_v6_data;

drop table network_traffic;
