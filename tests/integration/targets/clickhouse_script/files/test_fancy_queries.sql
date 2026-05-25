--- Lamba + arrays
SELECT 
    arrayMap(x -> x * 2, [1,2,3]) as doubled,
    arrayFilter(x -> x > 5, doubled) as filtered,
    arrayReduce('sum', filtered) as summed;
-- Window Functions
SELECT 
    number,
    lagInFrame(number, 1, 0) OVER (ORDER BY number) AS lagged,
    leadInFrame(number, 1, 0) OVER (ORDER BY number) AS leaded,
    number - lagInFrame(number, 1, 0) OVER (ORDER BY number) AS diff,
    row_number() OVER (ORDER BY number) AS row_num
FROM numbers(10);
-- Jsonextract
SELECT 
    JSONExtractString('{"a":"123"}', 'a') as str,
    JSONExtractInt('{"a":"123"}', 'a') as int,
    toTypeName(str) as str_type,
    toTypeName(int) as int_type;
-- Conditional aggregation
SELECT 
    sumIf(number, number % 2 = 0) as even_sum,
    avgIf(number, number > 5) as avg_gt5,
    groupArrayIf(number, number BETWEEN 3 AND 7) as arr
FROM numbers(10);
-- Recursive CTE
WITH RECURSIVE dates AS (
    SELECT today() as dt
    UNION ALL
    SELECT dt - INTERVAL 1 DAY
    FROM dates
    WHERE dt > today() - INTERVAL 7 DAY
)
SELECT dt, toDayOfWeek(dt) as dow FROM dates;
-- Tuple/array unpack
SELECT 
    (number, number * 2) AS tup, 
    tup.1 AS first, 
    tup.2 AS second 
FROM numbers(5) 
WHERE (first, second) IN ((1, 2), (2, 4), (3, 6));
-- Bitwise
SELECT 
    bitAnd(15, 7) as and_op,
    bitOr(8, 3) as or_op,
    bitXor(5, 3) as xor_op,
    bitShiftLeft(1, 3) as shl,
    bitShiftRight(8, 2) as shr;
-- URL function and extract
SELECT 
    'https://clickhouse.com/docs?q=test&page=1' as url,
    protocol(url) as proto,
    domain(url) as domain,
    path(url) as path,
    queryString(url) as qs,
    extractURLParameter(url, 'q') as param_q;
-- Polymorphic table functions
SELECT * FROM (
    SELECT toUInt64(number) AS value FROM numbers(3) 
    UNION ALL 
    SELECT toUInt64(0) FROM (SELECT 'a' AS string) 
    UNION ALL 
    SELECT toUInt64(1) 
) ORDER BY value;
-- Array join
SELECT 
    arr,
    arr_value,
    index
FROM (
    SELECT [1,2,3] as arr
)
ARRAY JOIN arr as arr_value, arrayEnumerate(arr) as index;
-- With + CTEs
WITH 
    data AS (SELECT number FROM numbers(100)),
    filtered AS (SELECT number FROM data WHERE number % 10 = 0),
    stats AS (SELECT count() as cnt, sum(number) as total FROM filtered)
SELECT cnt, total, total/cnt as avg FROM stats;
-- Conditional with type juggling
SELECT 
    multiIf(1 < 2, 'a', 2 < 3, 'b', 'c') as multi,
    if(1, 42, NULL) as if_null,
    coalesce(NULL, NULL, 'first_non_null') as coalesced,
    assumeNotNull(42) as assumed;
-- Ip address + cidrs
SELECT 
    IPv4StringToNum('192.168.1.1') as ip_num,
    IPv4NumToString(3232235521) as ip_str,
    IPv4CIDRToRange(toIPv4('192.168.1.0'), 24) as cidr_range,
    toIPv4('10.0.0.1') as ip_type;
-- Hashes
SELECT 
    cityHash64('hello') as hash64,
    sipHash64('world') as sip,
    farmFingerprint64('test') as fingerprint,
    halfMD5('data') as half_md5,
    URLHash('https://example.com') as url_hash;
-- Aggregation with rescampling
SELECT 
    number,
    avg(number) OVER (ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg,
    count() OVER (PARTITION BY number % 3) as bucket_count
FROM numbers(10);
-- Geodistance
SELECT 
    greatCircleDistance(0.0, 0.0, 10.0, 10.0) as dist_meters,
    pointInEllipses(5.0, 5.0, 0.0, 0.0, 10.0, 10.0) as in_ellipse,
    geoDistance(0.0, 0.0, 10.0, 10.0) as geo_dist;
-- Random
SELECT 
    rand() as r1,
    randConstant() as r2,
    rand64() as r3,
    generateUUIDv4() as uuid,
    now64() as ts_with_ms;
-- Nested conditional
SELECT 
    toFloat64(toString(42)) as cast_chain,
    reinterpretAsInt8(reinterpretAsFloat32(42)) as reinterpret,
    toDecimal32('123.45', 2) as decimal_val;
-- Null handling
SELECT 
    [1,2,NULL,3] as arr,
    arr[3] as null_elem,
    arr[10] as out_of_range,
    arr[1] as first,
    arrayExists(x -> x IS NULL, arr) as has_null;
-- Over-engineering
SELECT groupArray(day)[1] as first_day, sum(visits) as total FROM (SELECT toDate(event_time) as day, count() as visits FROM (SELECT arrayJoin([now(), now()-3600, now()-7200]) as event_time) GROUP BY day) HAVING total > 0 ORDER BY first_day WITH FILL FROM today() - INTERVAL 3 DAY TO today() STEP INTERVAL 1 DAY;