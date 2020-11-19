local librdkafka = require 'rdkafka.librdkafka'
local ffi = require 'ffi'

local KafkaTopicPartitionList = {}
KafkaTopicPartitionList.__index = KafkaTopicPartitionList

function KafkaTopicPartitionList.create(topic_cnt, original_config)
    local config = {}
    setmetatable(config, KafkaTopicPartitionList)

    if original_config and original_config.topic_partition_list_ then
        rawset(config, "topic_partition_list_", librdkafka.rd_kafka_topic_partition_list_copy(original_config.topic_partition_list_))
    else
        rawset(config, "topic_partition_list_", librdkafka.rd_kafka_topic_partition_list_new(topic_cnt))
    end
    ffi.gc(config.topic_partition_list_, function(config)
        librdkafka.rd_kafka_topic_partition_list_destroy(config)
    end
    )

    return config
end

function KafkaTopicPartitionList:__newindex(name, value)
    assert(self.topic_partition_list_ ~= nil)
    librdkafka.rd_kafka_topic_partition_list_add(self.topic_partition_list_, name, tonumber(value))
end

return KafkaTopicPartitionList