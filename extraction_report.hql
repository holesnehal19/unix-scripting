CREATE TABLE IF NOT EXISTS secure_analysis.load_uuid_processed_file
(
        srno int,
        uuid varchar(40),
        decrypt_status varchar(40),
        paypal_identifier int,
        actual_paypal_identifier varchar(40),
        hmac_match_status varchar(40),
        properties varchar(40),
        exception varchar(40),
        comments varchar(40),
        tenant varchar(40),
        domain varchar(40),
        table_name varchar(40)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\020';
LOAD DATA LOCAL INPATH '${hiveconf:file_path}' INTO TABLE secure_analysis.load_uuid_processed_file;
CREATE EXTERNAL TABLE IF NOT EXISTS secure_analysis.uuid_report
(
        srno int,
        uuid varchar(40),
        decrypt_status varchar(40),
        paypal_identifier int,
        actual_paypal_identifier varchar(40),
        hmac_match_status varchar(40),
        properties varchar(40),
        exception varchar(40),
        comments varchar(40)
)
PARTITIONED BY (tenant varchar(40),domain_name varchar(40),table_name varchar(40),year varchar(40),month varchar(40),day varchar(40),time varchar(40))
CLUSTERED BY (uuid) INTO 32 BUCKETS
STORED AS PARQUET
LOCATION 'hdfs://tahoe/sys/datalake/secure_analysis/uuid_report';
INSERT INTO secure_analysis.uuid_report PARTITION(tenant='${hiveconf:tenant}',domain_name='${hiveconf:domain_name}',table_name='${hiveconf:table_name}',year=${hiveconf:year},month=${hiveconf:month},day=${hiveconf:day},time=${hiveconf:current_time})
SELECT
        srno,
        uuid,
        decrypt_status,
        paypal_identifier,
        actual_paypal_identifier,
        hmac_match_status,
        properties,
        exception,
        comments
FROM secure_analysis.load_uuid_processed_file;
DROP TABLE IF EXISTS secure_analysis.load_uuid_processed_file;
