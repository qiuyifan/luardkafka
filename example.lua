local cjson = require 'cjson'

local function testconsumer()
    -- create rd_kafka_conf_t
    local config = require 'rdkafka.config'.create()

    config["bootstrap.servers"] = "kafka0.test.mp.sohuno.com:9092,kafka1.test.mp.sohuno.com:9092,kafka2.test.mp.sohuno.com:9092"
    config["group.id"] = "group.test"
    config["auto.offset.reset"] = "earliest"

    -- create rd_kafka_t consumer
    local consumer = require 'rdkafka.consumer'.create(config)

    -- create subscription
    -- ignored partition by subscribe
    local KAFKA_PARTITION_UA = -1

    local sub_topics = {"test", "test1"};
    local subscription = require 'rdkafka.topic_partition_list'.create(#sub_topics)
    for i = 1, #sub_topics do
        subscription[sub_topics[i]] = KAFKA_PARTITION_UA
    end

    -- subscribe
    consumer:subscribe(subscription)

    -- poll
    local circle = 0
    while (circle < 500) do
        local ret = consumer:poll(100)
        if ret and type(ret) == 'table' and next(ret) then
            local pollinfo = cjson.encode(ret)
            print("consumer poll info:" .. pollinfo .. '<br>')
        end
        circle = circle + 1
    end
end

local function testproducer()
    local config = require 'rdkafka.config'.create()

    config["statistics.interval.ms"] = "100"
    config["bootstrap.servers"] = "kafka0.test.mp.sohuno.com:9092,kafka1.test.mp.sohuno.com:9092,kafka2.test.mp.sohuno.com:9092"
    config:set_delivery_cb(function (payload, err) print("Delivery Callback '"..payload.."'") end)
    config:set_stat_cb(function (payload) print("Stat Callback '"..payload.."'") end)

    local producer = require 'rdkafka.producer'.create(config)
    local topic_config = require 'rdkafka.topic_config'.create()
    topic_config["auto.commit.enable"] = "true"

    local topic = require 'rdkafka.topic'.create(producer, "test", topic_config)

    local KAFKA_PARTITION_UA = -1

    for i = 0,10 do
        producer:produce(topic, KAFKA_PARTITION_UA, "this is test message"..tostring(i))
    end

    while producer:outq_len() ~= 0 do
        producer:poll(10)
    end
end