#include <iostream>
#include <random>
#include <functional>
#include <algorithm>
#include "librdkafka/rdkafkacpp.h"
#include <thread>

RdKafka::Producer *producer;

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) override {
        std::cout << "Message delivery for (" << message.len() << " bytes): " <<
                  message.errstr() << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};

static bool run = true;

std::string* random_string(size_t length)
{
    auto randchar = []() -> char
    {
        const char charset[] =
                "0123456789"
                        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string* str =  new std::string(length, 0);
    std::generate_n((*str).begin(), length, randchar);
    return str;
}

void pollAll() {
    while(run)
        producer->poll(5);
}

int main() {
    std::string brokers = "127.0.0.1:9091, 127.0.0.1:9092, 127.0.0.1:9093";
    std::string errstr;
    std::string topic_str = "telemetry-replicated";
    int32_t partition = RdKafka::Topic::PARTITION_UA;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

    conf->set("metadata.broker.list", brokers, errstr);

    ExampleDeliveryReportCb ex_dr_cb;

    conf->set("dr_cb", &ex_dr_cb, errstr);


    producer = RdKafka::Producer::create(conf, errstr);
    if (!producer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        exit(1);
    }

    std::cout << "% Created producer " << producer->name() << std::endl;

    std::thread pooling(pollAll);

    RdKafka::Topic *topic = RdKafka::Topic::create(producer, topic_str,
                                                   tconf, errstr);
    if (!topic) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        exit(1);
    }

    for (std::string line; run && std::getline(std::cin, line);) {

        RdKafka::ErrorCode resp =
                producer->produce(topic, partition,
                                  RdKafka::Producer::RK_MSG_COPY,
                                  const_cast<char *>(line.c_str()), line.size(),
                                  random_string(32), nullptr);
        if (resp != RdKafka::ERR_NO_ERROR)
            std::cerr << "% Produce failed: " <<
                      RdKafka::err2str(resp) << std::endl;
        else
            std::cerr << "% Produced message (" << line.size() << " bytes)" <<
                      std::endl;
    }

    while (run && producer->outq_len() > 0) {
        std::cerr << "Waiting for " << producer->outq_len() << std::endl;
        producer->poll(1000);
    }

    run = false;

    delete topic;
    delete producer;

    getchar();
}
