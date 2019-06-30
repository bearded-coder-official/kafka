<?php

namespace App\Console\Commands;

use Illuminate\Console\Command;
use Ramsey\Uuid\Uuid;

class ProduceAdCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'ad:produce';

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
        $rkProducer = new \RdKafka\Producer();
        $rkProducer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $rkProducer->newTopic(env('KAFKA_ADS_TOPIC_QUEUE_NAME'));

        $payload = [];
        $payload['id'] = Uuid::uuid4()->toString();
        $payload['raw_id'] = Uuid::uuid4()->toString();
        $payload['advertiser_id'] = Uuid::uuid4()->toString();
        $payload['manager_id'] = Uuid::uuid4()->toString();
        $payload['website_id'] = Uuid::uuid4()->toString();
        $payload['publisher_id'] = Uuid::uuid4()->toString();
        $payload['subscriber_id'] = Uuid::uuid4()->toString();
        $payload['actions'] = 'abc';
        $payload['badge'] = 'qwe';
        $payload['body'] = 'rty';
        $payload['data'] = 'uio';
        $payload['dir'] = 'ewq';
        $payload['lang'] = 'ytr';
        $payload['tag'] = 'poi';
        $payload['icon'] = 'zxc';
        $payload['image'] = 'asd';
        $payload['renotify'] = 'xvb';
        $payload['require_interaction'] = 'hyn';
        $payload['silent'] = 'iun';
        $payload['timestamp'] = '67576576';
        $payload['title'] = 'rurururue';
        $payload['vibrate'] = 'scsaac';
        $payload['type'] = 'cpc';
        $payload['external_price'] = 10;
        $payload['internal_price'] = 5;
        $payload['url'] = 'http://revquake.com';
        $payload['created_at'] = date('Y-m-d H:i:s');

        $json = json_encode($payload);

        echo $json . PHP_EOL;

        for ($i = 1; $i <= 1; $i++) {
            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
        }
    }
}
