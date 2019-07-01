<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Ramsey\Uuid\Uuid;

class ProducerStatisticCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'stat:produce';

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
     * @throws \Exception
     */
    public function handle()
    {
        $rkProducer = new \RdKafka\Producer();
        $rkProducer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $rkProducer->newTopic(env('KAFKA_STATISTICS_TOPIC_QUEUE_NAME'));

        $payload = [];
        $payload['id'] = Uuid::uuid4()->toString();
        $payload['ua'] = 'Mozilla/5.0 (Linux; Android 9; moto g(6) play) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Mobile Safari/537.36';
        $payload['ip'] = '170.80.224.3';
        $payload['event'] = rand(0, 1);
        $payload['language'] = 'pt-BR';
        $payload['triggered_at'] = date('Y-m-d H:i:s');
        $payload['push_subscriber_id'] = '004b658c-b5e6-4dc2-9b7e-6c06ca4fc273';
        $payload['advertiser_id'] = Uuid::uuid4()->toString();
        $payload['publisher_id'] = Uuid::uuid4()->toString();
        $payload['push_website_id'] = Uuid::uuid4()->toString();
        $payload['type'] = 'cpc';
        $payload['external_price'] = 0.10;
        $payload['internal_price'] = 0.08;

        $json = json_encode($payload);

        echo $json . PHP_EOL;

        for ($i = 1; $i <= 1; $i++) {
            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
        }
    }
}
