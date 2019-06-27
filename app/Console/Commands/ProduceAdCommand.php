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

        $topic->produce(\RD_KAFKA_PARTITION_UA, 0, '{"id":"15664c8d-5abc-4fee-8c7b-cae687be8366","raw_id":"bfcea7f1-9db9-4756-af80-0562c03584d0","manager_id":"","website_id":"","advertiser_id":"7e6ab572-ec80-48a2-9de4-204cb20f910d","publisher_id":"0.058,\"response\":\"{\n\"title\": \"Best B","subscriber_id":"","actions":"","badge":"","body":"","data":"","dir":"","lang":"","tag":"","icon":"https://cyneburg-yam.com/imp/5c5385a6-981a-11e9-90a8-1246877fad8e/1/jvJPZ7MkJQpeWejLFoAHc57rV_t16qFGXz5NIY33sNrLGWtrQcp8SawJ6Yq8BOgYq6c3wT4cBhzQWW2koPdkG4ye3J0KoRYoHnBfHYR3IQicA26Z-fqeYpI4RnY6-ucvBQS_EsIo2IUF_7yJz6guyOvE0cvFYPVfh9zLjnZ-eh5BsN8u145FmHmvVtFDBOl-5bUjHYI98avB9_XqFUT8EQ76tDjxan5wWLugni7LG3WX5DWLq4lK9ZDIzmwoawISIb5pqi0utaNLw-T9P9I3YxlpN11Vc-eUtu5FHJYPyuGR5ezJDjqk1ZN_MsWFVRfulTCb_uXpZDrW1X-Eh3dPnI1d42h1rvBh7Fw6BDHcmQOrB9NEKXxdzUL2QSDAD4BEyBevrpyEIzTf37ETx0y6XtfR-oo1tGMhym_pmJy5hsP_2SmfiNRKbqTztFiqlDj3DBJKNygHtf1ht2jrGXEyUS86N8SsRAPzH7NnbUBEIGBBqGLQgVuAPv1kQ0Xh2aEC8l9CjLAzErq9hpBnZy9oVOSbMo6ad0oFG7qDv6zAU_NN3_IdzZUh0b12Aw35QXve66qR7ryMXGKQYn9j4YQoJIntWbwab161cWxNytRH6RXAlA0zqy0IBlAQqFY0QSnSk_Xj0Rze7QmlocW3GfujGbX_xEm3CGXRmTR3xd7ZtSi8KNHPzRuRDMAt5dDz3EoDlBQsAw7wdYRjZ55TyVI2uHpHZBx305pmgkkAGm6ufU-WFOCxkkS9duO_Vs4UmejbRFe8GZsKpSlkObiNdrY3gLA-ZqCVmFlDo2mI-cs8ByC0ZuKOonTomo0zn2zslw==.MMRkNh-qSMV-0EAau0ImdA==","image":"","renotify":"","require_interaction":"","silent":"","timestamp":"","title":"Best Biz Phone Service is Here","vibrate":"","type":"","external_price":0.058,"internal_price":0.0464,"url":"https://cyneburg-yam.com/c/5c5385a6-981a-11e9-90a8-1246877fad8e/1/jvJPZ7MkJQpeWejLFoAHc57rV_t16qFGXz5NIY33sNrLGWtrQcp8SawJ6Yq8BOgYq6c3wT4cBhzQWW2koPdkG4ye3J0KoRYoHnBfHYR3IQicA26Z-fqeYpI4RnY6-ucvBQS_EsIo2IUF_7yJz6guyOvE0cvFYPVfh9zLjnZ-eh5BsN8u145FmHmvVtFDBOl-5bUjHYI98avB9_XqFUT8EQ76tDjxan5wWLugni7LG3WX5DWLq4lK9ZDIzmwoawISIb5pqi0utaNLw-T9P9I3YxlpN11Vc-eUtu5FHJYPyuGR5ezJDjqk1ZN_MsWFVRfulTCb_uXpZDrW1X-Eh3dPnI1d42h1rvBh7Fw6BDHcmQOrB9NEKXxdzUL2QSDAD4BEyBevrpyEIzTf37ETx0y6XtfR-oo1tGMhym_pmJy5hsP_2SmfiNRKbqTztFiqlDj3DBJKNygHtf1ht2jrGXEyUS86N8SsRAPzH7NnbUBEIGBBqGLQgVuAPv1kQ0Xh2aEC8l9CjLAzErq9hpBnZy9oVOSbMo6ad0oFG7qDv6zAU_NN3_IdzZUh0b12Aw35QXve66qR7ryMXGKQYn9j4YQoJIntWbwab161cWxNytRH6RXAlA0zqy0IBlAQqFY0QSnSk_Xj0Rze7QmlocW3GfujGbX_xEm3CGXRmTR3xd7ZtSi8KNHPzRuRDMAt5dDz3EoDlBQsAw7wdYRjZ55TyVI2uHpHZBx305pmgkkAGm6ufU-WFOCxkkS9duO_Vs4UmejbRFe8GZsKpSlkObiNdrY3gLA-ZqCVmFlDo2mI-cs8ByC0ZuKOonTomo0zn2zslw==.MMRkNh-qSMV-0EAau0ImdA==","created_at":"2019-06-26 13:57:39"}');
    }
}
