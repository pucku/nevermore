deleteres jars;
add jar hdfs:/tmp/avrodemo.jar;

drop stream avro_input;
create stream avro_input(
    json_value string
)
tblproperties(
    "source"="custom",
    "slipstream.datasource.classpath"="com.datasource.AvroDataInputSource",
    "slipstream.datasource.brokerlist"="tdh-24:9092",
    "slipstream.datasource.groupid"="group1a",
    "slipstream.datasource.registryurl"="http://tdh-24:18081",
    "slipstream.datasource.topic"="wtest"
);

set character.literal.as.string=true;

drop stream stream_mid;
create stream stream_mid as
select
 t1.id,
 t2.letter
from avro_input
lateral view json_tuple(t.json_value, "id") t1 as id
lateral view json_tuple(t.json_value, "letter") t2 as letter;

drop table r_mq;
create table r_mq(
    id string,
    letter string
);

drop streamjob job_avro_input;
create streamjob job_avro_input as ("insert into r_mq select * from stream_mid")
jobproperties("morphling.result.auto.flush"="true");

start streamjob job_avro_input;
