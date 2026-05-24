select 1;
select 1;
SELECT 
    IPv4StringToNum('192.168.1.1') as ip_num,
    IPv4NumToString(3232235521) as ip_str,
    IPv4CIDRToRange('192.168.1.0', 24) as cidr_range,
    toIPv4('10.0.0.1') as ip_type;
select 1;