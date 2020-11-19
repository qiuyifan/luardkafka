local librdkafka = require 'rdkafka.librdkafka'
local KafkaConfig = require 'rdkafka.config'
local KafkaTopic = require 'rdkafka.topic'
local KakfaTopicPartitionList = require 'rdkafka.topic_partition_list'
local ffi = require 'ffi'

local DEFAULT_DESTROY_TIMEOUT_MS = 3000
local DEFAULT_POLL_TIMEOUT_MS = 3000

local KafkaConsumer = {}
KafkaConsumer.__index = KafkaConsumer

-- [[ creates a new Kafka consumer]]
function KafkaConsumer.create(kafka_config, destroy_timeout_ms)
    local config = nil
    if kafka_config ~= nil then
        config = KafkaConfig.create(kafka_config).kafka_conf_
        ffi.gc(config, nil)
    end

    local ERRLEN = 256
    local errbuf = ffi.new("char[?]", ERRLEN)
    local kafka = librdkafka.rd_kafka_new(librdkafka.RD_KAFKA_CONSUMER, config, errbuf, ERRLEN)

    if kafka == nil then
        error(ffi.string(errbuf))
    end

    local consumer = {kafka_ = kafka}

    setmetatable(consumer, KafkaConsumer)
    ffi.gc(consumer.kafka_, function(...)
        librdkafka.rd_kafka_consumer_close(...)
        librdkafka.rd_kafka_destroy(...)
        librdkafka.rd_kafka_wait_destroyed(destroy_timeout_ms or DEFAULT_DESTROY_TIMEOUT_MS)
    end
    )

    -- set consumer
    consumer:poll_set_consumer()

    return consumer
end

function KafkaConsumer:poll_set_consumer()
    assert(self.kafka_ ~= nil)

    librdkafka.rd_kafka_poll_set_consumer(self.kafka_)
end

function KafkaConsumer:subscribe(subscription)
    assert(self.kafka_ ~= nil)
    assert(subscription ~= nil)
    if subscription ~= nil then
        config = KakfaTopicPartitionList.create(1, subscription).topic_partition_list_
        ffi.gc(config, nil)
    end

    local result = librdkafka.rd_kafka_subscribe(self.kafka_, config)
    librdkafka.rd_kafka_topic_partition_list_destroy(config)
    if result ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        error(ffi.string(librdkafka.rd_kafka_err2str(result)))
    end
    return result
end


function KafkaConsumer:poll(timeout)
    assert(self.kafka_ ~= nil)
    local ret = {}
    local rkm = librdkafka.rd_kafka_consumer_poll(self.kafka_, timeout or DEFAULT_POLL_TIMEOUT_MS)
    if rkm == nil then
        return ret
    end

    ret.err = tonumber(rkm.err)

    if rkm.err ~= librdkafka.RD_KAFKA_RESP_ERR_NO_ERROR then
        ret.errmsg = ffi.string(librdkafka.rd_kafka_message_errstr(rkm))
        librdkafka.rd_kafka_message_destroy(rkm)
        return ret
    end

    ret.topic = ffi.string(librdkafka.rd_kafka_topic_name(rkm.rkt))
    ret.partition = tonumber(rkm.partition)
    ret.offset = tonumber(rkm.offset)

    ret.key = ffi.string(rkm.key, tonumber(rkm.key_len))
    ret.payload = ffi.string(rkm.payload, tonumber(rkm.len))

    librdkafka.rd_kafka_message_destroy(rkm)
    return ret
end
jit.off(KafkaConsumer.poll)

return KafkaConsumer