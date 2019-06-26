<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Ramsey\Uuid\Uuid;

class ProduceRawCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'raw:produce';

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

    public function handle()
    {
        $rkProducer = new \RdKafka\Producer();
        $rkProducer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $rkProducer->newTopic(env('KAFKA_RAWS_TOPIC_QUEUE_NAME'));

        $payload = [];
        $payload['id'] = Uuid::uuid4()->toString();
        $payload['advertiser_id'] = Uuid::uuid4()->toString();
        $payload['publisher_id'] = Uuid::uuid4()->toString();
        $payload['request'] = base64_encode('http://revquake.com/');
        $payload['payload'] = null;
        $payload['response'] = null;
        $payload['created_at'] = date('Y-m-d H:i:s');

        $json = base64_encode(json_encode($payload));

        echo $json . PHP_EOL;

        $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
    }
}
