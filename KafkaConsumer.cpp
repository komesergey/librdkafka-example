
#include <iostream>
#include <csignal>
#include <cstring>
#include <sys/time.h>
#include "librdkafka/rdkafkacpp.h"

static bool run = true;
static bool exit_eof = false;
static int eof_cnt = 0;
static int partition_cnt = 0;
static int verbosity = 3;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
std::vector<RdKafka::TopicPartition*> offsets;

static void sigterm(int sig) {
	run = false;
}


static void print_time() {
    struct timeval tv;
    char buf[64];
    gettimeofday(&tv, nullptr);
    strftime(buf, sizeof(buf) - 1, "%Y-%m-%d %H:%M:%S", localtime(&tv.tv_sec));
    fprintf(stderr, "%s.%03d: ", buf, (int)(tv.tv_usec / 1000));
}

class ExampleEventCb : public RdKafka::EventCb {
public:
	void event_cb(RdKafka::Event &event) override {

		print_time();

		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				run = false;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",
				event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		case RdKafka::Event::EVENT_THROTTLE:
			std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
				event.broker_name() << " id " << (int)event.broker_id() << std::endl;
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
				" (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			break;
		}
	}
};


class ExampleRebalanceCb : public RdKafka::RebalanceCb {
private:
	static void part_list_print(const std::vector<RdKafka::TopicPartition*>&partitions) {
		for (unsigned int i = 0; i < partitions.size(); i++)
			std::cerr << partitions[i]->topic() <<
			"[" << partitions[i]->partition() << "], ";
		std::cerr << "\n";
	}

public:
	void rebalance_cb(RdKafka::KafkaConsumer *consumer,
		RdKafka::ErrorCode err,
		std::vector<RdKafka::TopicPartition*> &partitions) override {
		std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

		part_list_print(partitions);

		if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
			consumer->assign(partitions);
			partition_cnt = (int)partitions.size();
		}
		else {
			consumer->unassign();
			partition_cnt = 0;
		}
		offsets = partitions;
		eof_cnt = 0;
	}
};


RdKafka::ErrorCode msg_consume(RdKafka::Message* message, void* opaque) {
	switch (message->err()) {
	case RdKafka::ERR__TIMED_OUT:
		return RdKafka::ERR__TIMED_OUT;

	case RdKafka::ERR_NO_ERROR: {
		/* Real message */
		msg_cnt++;
		msg_bytes += message->len();
		if (verbosity >= 3)
			std::cerr << "Read msg at offset " << message->offset() << std::endl;

		RdKafka::MessageTimestamp ts = message->timestamp();
		if (verbosity >= 2 &&
			ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
			std::string tsname = "?";
			if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
				tsname = "create time";
			else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
				tsname = "log append time";
			std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
		}
		if (verbosity >= 2 && message->key()) {
			std::cout << "Key: " << *message->key() << std::endl;
		}
		if (verbosity >= 1) {
			printf("%.*s\n",
				   static_cast<int>(message->len()),
				   static_cast<const char *>(message->payload()));
		}
		return RdKafka::ERR_NO_ERROR;
	}
	case RdKafka::ERR__PARTITION_EOF:
		if (exit_eof && ++eof_cnt == partition_cnt) {
			std::cerr << "%% EOF reached for all " << partition_cnt <<
					  " partition(s)" << std::endl;
			run = false;
		}
		return RdKafka::ERR__PARTITION_EOF;

	case RdKafka::ERR__UNKNOWN_TOPIC:
        return RdKafka::ERR__UNKNOWN_TOPIC;

	case RdKafka::ERR__UNKNOWN_PARTITION:
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
		return RdKafka::ERR__UNKNOWN_PARTITION;
	default:
		/* Errors */
		std::cerr << "Consume failed: " << message->errstr() << std::endl;
		run = false;
	}
}


class ExampleConsumeCb : public RdKafka::ConsumeCb {
public:
	void consume_cb(RdKafka::Message &msg, void *opaque) override {
		msg_consume(&msg, opaque);
	}
};



int main(int argc, char **argv) {
	std::string brokers = "localhost:9091,localhost:9092,localhost:9093";
	std::string topic = "telemetry-replicated";
	std::string errstr;
	std::string topic_str;
	std::string mode;
	std::string debug = "consumer,generic";
	std::vector<std::string> topics;
	topics.push_back(topic);

	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	ExampleRebalanceCb ex_rebalance_cb;
	conf->set("rebalance_cb", &ex_rebalance_cb, errstr);

	if (conf->set("group.id", "group1", errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	/*
	* Set configuration properties
	*/
	conf->set("metadata.broker.list", brokers, errstr);
	tconf->set("topic.enable.auto.commit", "false", errstr);
	conf->set("enable.auto.commit", "false", errstr);
	conf->set("auto.offset.reset", "error", errstr);
	conf->set("enable.auto.offset.store", "false", errstr);
	conf->set("offset.store.method", "file", errstr);
	conf->set("offset.store.sync.interval.ms", "0", errstr);
	conf->set("offset.store.path", "/home/tony/Kafka/offsets", errstr);
	tconf->set("topic.offset.store.path", "/home/tony/Kafka/offsets", errstr);
	tconf->set("topic.offset.store.method", "file", errstr);
	tconf->set("offset.store.method", "file", errstr);
	tconf->set("enable.auto.offset.store", "false", errstr);
	tconf->set("topic.offset.store.sync.interval.ms", "0", errstr);


	if (!debug.empty()) {
		if (conf->set("debug", debug, errstr) != RdKafka::Conf::CONF_OK) {
			std::cerr << errstr << std::endl;
			exit(1);
		}
	}

	ExampleConsumeCb ex_consume_cb;


	ExampleEventCb ex_event_cb;
	conf->set("event_cb", &ex_event_cb, errstr);

	conf->set("default_topic_conf", tconf, errstr);
	delete tconf;

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	RdKafka::KafkaConsumer *consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (!consumer) {
		std::cerr << "Failed to create consumer: " << errstr << std::endl;
		exit(1);
	}

	delete conf;

	std::cout << "% Created consumer " << consumer->name() << std::endl;

	RdKafka::ErrorCode err = consumer->subscribe(topics);
	if (err) {
		std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
			<< RdKafka::err2str(err) << std::endl;
		exit(1);
	}

	while (run) {
		RdKafka::Message *msg = consumer->consume(1000);
		RdKafka::ErrorCode err2 = msg_consume(msg, nullptr);
		if (err2 == RdKafka::ERR_NO_ERROR) {
			consumer->assignment(offsets);
			std::cout << "Partitions: " << offsets[0]->partition() << " " << msg->partition() << std::endl;

			for (unsigned int i = 0; i < offsets.size(); i++) {
				if (offsets[i]->partition() == msg->partition()) {
					std::cout << "Partition was found: " << offsets[i]->partition() << " " << std::endl;
					offsets[i]->set_offset(msg->offset());
					std::cout << "Received offset: " << msg->offset() << " " << std::endl;
				}
			}

			RdKafka::ErrorCode err1 = consumer->offsets_store(offsets);
			std::cout << "Store error: " << RdKafka::err2str(err1) << " " << std::endl;
			consumer->commitSync(msg);
		}
		delete msg;
	}

	consumer->close();
	delete consumer;

	std::cerr << "% Consumed " << msg_cnt << " messages (" << msg_bytes << " bytes)" << std::endl;

	RdKafka::wait_destroyed(5000);

	return 0;
}

