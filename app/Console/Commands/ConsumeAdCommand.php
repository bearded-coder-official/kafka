<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Ramsey\Uuid\Uuid;

class ConsumeAdCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ad:consume';

    /**
     * The console command description.
     *
     * @var string
     */
    protected $description = 'Command description';

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Execute the console command.
     *
     * @return mixed
     * @throws \RdKafka\Exception
     */
    public function handle()
    {
        $kafkaConfiguration = new \RdKafka\Conf();

        $kafkaConfiguration->setRebalanceCb(function (\RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case \RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    echo "\n ERROR: RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS\n";
                    $kafka->assign($partitions);
                    break;

                case \RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    echo "\n ERROR: RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS\n";
                    $kafka->assign(NULL);
                    break;

                default:
                    throw new \Exception($err);
            }
        });

        $kafkaConfiguration->set('group.id', env('KAFKA_ADS_TOPIC_QUEUE_GROUP'));
        $kafkaConfiguration->set('metadata.broker.list', env('KAFKA_BROKERS'));

        $topicConfiguration = new \RdKafka\TopicConf();
        $topicConfiguration->set('auto.commit.interval.ms', 100);
        $kafkaConfiguration->setDefaultTopicConf($topicConfiguration);

        $consumer = new \RdKafka\KafkaConsumer($kafkaConfiguration);
        $consumer->subscribe([env('KAFKA_ADS_TOPIC_QUEUE_NAME')]);

        $producer = new \RdKafka\Producer();
        $producer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $producer->newTopic(env('KAFKA_ADS_TOPIC_NAME'));

        while (true) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    $payload = json_decode($message->payload, true);

                    $data = [];
                    $data['id'] = $payload['id'];
                    $data['raw_id'] = $payload['raw_id'];
                    $data['advertiser_id'] = $payload['advertiser_id'];
                    $data['website_id'] = $payload['website_id'];
                    $data['publisher_id'] = $payload['publisher_id'];
                    $data['subscriber_id'] = $payload['subscriber_id'];
                    $data['actions'] = $payload['actions'];
                    $data['badge'] = $payload['badge'];
                    $data['body'] = $payload['body'];
                    $data['data'] = $payload['data'];
                    $data['dir'] = $payload['dir'];
                    $data['lang'] = $payload['lang'];
                    $data['tag'] = $payload['tag'];
                    $data['icon'] = $payload['icon'];
                    $data['image'] = $payload['image'];
                    $data['renotify'] = $payload['renotify'];
                    $data['require_interaction'] = $payload['require_interaction'];
                    $data['silent'] = $payload['silent'];
                    $data['timestamp'] = $payload['timestamp'];
                    $data['title'] = $payload['title'];
                    $data['vibrate'] = $payload['vibrate'];
                    $data['type'] = $payload['type'];
                    $data['external_price'] = $payload['external_price'];
                    $data['internal_price'] = $payload['internal_price'];
                    $data['url'] = $payload['url'];
                    $data['created_at'] = $payload['created_at'];
                    $data['manager_id'] = 'e6b39ed1-56ff-4079-87e0-dc22c7dc4a24'; // Vitalik
                    $data['project_id'] = '9f951b51-a60e-4c74-9346-35c68b66add3'; // RevQuake

                    $json = json_encode($data);

                    echo $json . PHP_EOL;

                    $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);

                    break;
                case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    echo "No more messages; will wait for more\n";
                    break;
                case \RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        }
    }
}
