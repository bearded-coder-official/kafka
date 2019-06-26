<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;

class ConsumeRawCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'raw:consume';

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
     */
    public function handle()
    {
        $conf = new \RdKafka\Conf();

        $conf->set('group.id', env('KAFKA_RAWS_TOPIC_QUEUE_GROUP'));
        $conf->set('offset.store.method', 'broker');

        $rk = new \RdKafka\Consumer($conf);
        $rk->addBrokers(env('KAFKA_BROKERS'));

        $rkProducer = new \RdKafka\Producer();
        $rkProducer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $rkProducer->newTopic(env('KAFKA_RAWS_TOPIC_NAME'));

        $queue = $rk->newQueue();

        $queueTopic = $rk->newTopic(env('KAFKA_RAWS_TOPIC_QUEUE_NAME'));
        $queueTopic->consumeQueueStart(0, \RD_KAFKA_OFFSET_STORED, $queue);
        $queueTopic->consumeQueueStart(1, \RD_KAFKA_OFFSET_STORED, $queue);
        $queueTopic->consumeQueueStart(2, \RD_KAFKA_OFFSET_STORED, $queue);

        while (true) {
            $msg = $queue->consume(1000);
            if (null === $msg) {
                continue;
            } else if ($msg->err) {
                echo $msg->errstr(), PHP_EOL;
                break;
            }

            $payload = json_decode(base64_decode($msg->payload), true);

            $data = [];
            $data['id'] = $payload['id'];
            $data['advertiser_id'] = $payload['advertiser_id'];
            $data['publisher_id'] = $payload['publisher_id'];
            $data['request'] = $payload['request'];
            $data['payload'] = $payload['payload'];
            $data['response'] = $payload['response'];
            $data['manager_id'] = 'e6b39ed1-56ff-4079-87e0-dc22c7dc4a24'; // Vitalik
            $data['project_id'] = '9f951b51-a60e-4c74-9346-35c68b66add3'; // RevQuake
            $data['created_at'] = $payload['created_at'];

            $json = json_encode($data);

            echo $json . PHP_EOL;

            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
        }
    }
}
