<?php

namespace App\Console\Commands;

use App\Http\Services\NotifyService;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Validator;
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

    protected $notifyService;

    /**
     * Create a new command instance.
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();

        $this->notifyService = new NotifyService();
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
                    $data['id'] = empty($payload['id']) ? null : $payload['id'];
                    $data['raw_id'] = empty($payload['raw_id']) ? null : $payload['raw_id'];
                    $data['advertiser_id'] = empty($payload['advertiser_id']) ? null : $payload['advertiser_id'];
                    $data['website_id'] = empty($payload['website_id']) ? null : $payload['website_id'];
                    $data['publisher_id'] = empty($payload['publisher_id']) ? null : $payload['publisher_id'];
                    $data['subscriber_id'] = empty($payload['subscriber_id']) ? null : $payload['subscriber_id'];
                    $data['actions'] = empty($payload['actions']) ? null : $payload['actions'];
                    $data['badge'] = empty($payload['badge']) ? null : $payload['badge'];
                    $data['body'] = empty($payload['body']) ? null : $payload['body'];
                    $data['data'] = empty($payload['data']) ? null : $payload['data'];
                    $data['dir'] = empty($payload['dir']) ? null : $payload['dir'];
                    $data['lang'] = empty($payload['lang']) ? null : $payload['lang'];
                    $data['tag'] = empty($payload['tag']) ? null : $payload['tag'];
                    $data['icon'] = empty($payload['icon']) ? null : $payload['icon'];
                    $data['image'] = empty($payload['image']) ? null : $payload['image'];
                    $data['renotify'] = empty($payload['renotify']) ? null : $payload['renotify'];
                    $data['require_interaction'] = empty($payload['require_interaction']) ? null : $payload['require_interaction'];
                    $data['silent'] = empty($payload['silent']) ? null : $payload['silent'];
                    $data['timestamp'] = empty($payload['timestamp']) ? null : $payload['timestamp'];
                    $data['title'] = empty($payload['title']) ? null : $payload['title'];
                    $data['vibrate'] = empty($payload['vibrate']) ? null : $payload['vibrate'];
                    $data['type'] = empty($payload['type']) ? null : $payload['type'];
                    $data['external_price'] = empty($payload['external_price']) ? null : $payload['external_price'];
                    $data['internal_price'] = empty($payload['internal_price']) ? null : $payload['internal_price'];
                    $data['url'] = empty($payload['url']) ? null : $payload['url'];
                    $data['created_at'] = empty($payload['created_at']) ? null : $payload['created_at'];
                    $data['manager_id'] = 'e6b39ed1-56ff-4079-87e0-dc22c7dc4a24'; // Vitalik
                    $data['project_id'] = '9f951b51-a60e-4c74-9346-35c68b66add3'; // RevQuake

                    $json = json_encode($data, JSON_PRETTY_PRINT);

                    $bag = $this->validate($data);
                    if ($bag->count() > 0) {
                        $this->notifyService->notify("RAW REQUEST PROBLEM", $bag->getMessages(), $json);
                        continue;
                    }

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

    protected function validate(array $message)
    {
        $validator = Validator::make($message, [
            'id' => 'required|uuid',
            'raw_id' => 'required|uuid',
            'advertiser_id' => 'nullable|uuid',
            'website_id' => 'nullable|uuid',
            'publisher_id' => 'nullable|uuid',
            'subscriber_id' => 'nullable|uuid',
            'actions' => 'nullable',
            'badge' => 'nullable',
            'body' => 'nullable',
            'data' => 'nullable',
            'dir' => 'nullable',
            'lang' => 'nullable',
            'tag' => 'nullable',
            'icon' => 'required|url',
            'image' => 'nullable|url',
            'renotify' => 'nullable',
            'require_interaction' => 'nullable',
            'silent' => 'nullable',
            'timestamp' => 'nullable',
            'title' => 'required|nullable',
            'vibrate' => 'nullable',
            'type' => 'in:cpc,cpa,cpi',
            'external_price' => 'required|numeric',
            'internal_price' => 'required|numeric|lte:external_price',
            'url' => 'required|url',
            'manager_id' => 'required|uuid',
            'project_id' => 'required|uuid',
            'created_at' => 'required|date_format:Y-m-d H:i:s',
        ]);

        return $validator->errors();
    }
}
