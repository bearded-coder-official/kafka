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
        $payload['Id'] = Uuid::uuid4()->toString();
        $payload['UA'] = 'Mozilla/5.0 (Linux; Android 9; moto g(6) play) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.157 Mobile Safari/537.36';
        $payload['IP'] = '170.80.224.3';
        $payload['Event'] = rand(0, 1);
        $payload['Language'] = 'pt-BR';
        $payload['TriggeredAt'] = date('Y-m-d H:i:s');

        $json = json_encode($payload);

        echo $json . PHP_EOL;

        for ($i = 1; $i <= 25; $i++) {
            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
        }
    }
}
