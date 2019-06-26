<?php

namespace App\Console\Commands;

use App\Subscriber;
use Illuminate\Console\Command;
use Ramsey\Uuid\Uuid;

class ConsumerStatisticCommand extends Command
{
    /**
     * The name and signature of the console command.
     *
     * @var string
     */
    protected $signature = 'stat:consume';

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
    public function handle(\Psr\Container\ContainerInterface $container)
    {
        $conf = new \RdKafka\Conf();

        $conf->set('group.id', env('KAFKA_STATISTICS_TOPIC_QUEUE_GROUP'));
        $conf->set('offset.store.method', 'broker');

        $rk = new \RdKafka\Consumer($conf);
        $rk->addBrokers(env('KAFKA_BROKERS'));

        $rkProducer = new \RdKafka\Producer();
        $rkProducer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $rkProducer->newTopic(env('KAFKA_STATISTICS_TOPIC_NAME'));

        $queue = $rk->newQueue();

        $queueTopic = $rk->newTopic(env('KAFKA_STATISTICS_TOPIC_QUEUE_NAME'));
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

            $adId = $payload['Id'];
            $adUa = $payload['UA'];
            $adIp = $payload['IP'];
            $adEvent = $payload['Event'];
            $riggeredAt = $payload['TriggeredAt'] ?? date('Y-m-d H:i:s');

            $dd = new \DeviceDetector\DeviceDetector($adUa);
            $dd->parse();

            $_city = $container->get('\GeoIp2\Database\Reader\City');
            $_country = $container->get('\GeoIp2\Database\Reader\Country');
            $_connectionType = $container->get('\GeoIp2\Database\Reader\ConnectionType');

            $_city = $_city->city($adIp);
            $_country = $_country->country($adIp);
            $_connectionType = $_connectionType->connectionType($adIp);

            $ad = \Illuminate\Support\Facades\Cache::get($adId);

            if (!$ad) {
//                echo "No ad!";
//                continue;

                $ad = [];
                $ad['id'] = Uuid::uuid4()->toString();
                $ad['raw_id'] = Uuid::uuid4()->toString();
                $ad['advertiser_id'] = Uuid::uuid4()->toString();
                $ad['manager_id'] = Uuid::uuid4()->toString();
                $ad['website_id'] = Uuid::uuid4()->toString();
                $ad['publisher_id'] = Uuid::uuid4()->toString();
                $ad['subscriber_id'] = Uuid::uuid4()->toString();
                $ad['actions'] = 'abc';
                $ad['badge'] = 'qwe';
                $ad['body'] = 'rty';
                $ad['data'] = 'uio';
                $ad['dir'] = 'ewq';
                $ad['lang'] = 'ytr';
                $ad['tag'] = 'poi';
                $ad['icon'] = 'zxc';
                $ad['image'] = 'asd';
                $ad['renotify'] = 'xvb';
                $ad['require_interaction'] = 'hyn';
                $ad['silent'] = 'iun';
                $ad['timestamp'] = '67576576';
                $ad['title'] = 'rurururue';
                $ad['vibrate'] = 'scsaac';
                $ad['type'] = 'cpc';
                $ad['external_price'] = 10;
                $ad['internal_price'] = 5;
                $ad['url'] = 'http://revquake.com';
                $ad['created_at'] = date('Y-m-d H:i:s');
            }

            $subscriberId = $ad['subscriber_id'];
            $user = \Illuminate\Support\Facades\Cache::remember( $ad['subscriber_id'], 8600, function () use ($subscriberId) {
                return Subscriber::find($subscriberId);
            });

            $os = $dd->getOs()['name'] ?? null;
            $language = $this->request['HTTP_ACCEPT_LANGUAGE'];
            $device = $dd->getDeviceName() ?? null;
            $brand = $dd->getBrandName() ?? null;
            $client_type = $dd->getClient()['type'] ?? null;
            $client_name = $dd->getClient()['name'] ?? null;
            $connection_type = $_connectionType->connectionType;
            $city_iso = (string)$_city->city->geonameId;
            $country_iso = $_country->country->isoCode;
            $continent_iso = $_country->continent->code;
            $timezone = $_city->location->timeZone;
            $utm_source = $user->utm_source ?? null;
            $utm_medium = $user->utm_medium ?? null;
            $utm_content = $user->utm_content ?? null;
            $driver = $user->driver ?? null;

            $ad = [];

            $data = [];
            $data['id'] = Uuid::uuid4()->toString();
            $data['subscriber_id'] = $ad['subscriber_id']; // redis
            $data['advertiser_id'] = $ad['advertiser_id']; // redis
            $data['ad_id'] = $adId;
            $data['publisher_id'] = $ad['publisher_id']; // redis
            $data['website_id'] = $ad['website_id']; // redis
            $data['manager_id'] = $ad['manager_id']; // redis
            $data['cost_type'] = $ad['type']; // redis
            $data['revenue'] = round($ad['external_price'], 4, PHP_ROUND_HALF_DOWN); // redis
            $data['cost'] = round($ad['internal_price'], 4, PHP_ROUND_HALF_DOWN); // redis
            $data['profit'] = round($ad['external_price'] - $ad['internal_price'], 4, PHP_ROUND_HALF_DOWN); // redis
            $data['click'] = $adEvent == 0 ? 0 : 1;
            $data['impression'] = $adEvent == 1 ? 0 : 1;
            $data['is_duplicate'] = 0;
            $data['is_hijack'] = 0;
            $data['is_fraud'] = 0;
            $data['is_test'] = 0;
            $data['ip'] = $adIp;
            $data['os'] = $os;
            $data['language'] = $language;
            $data['device'] = $device;
            $data['brand'] = $brand;
            $data['client_type'] = $client_type;
            $data['client_name'] = $client_name;
            $data['connection_type'] = $connection_type;
            $data['user_agent'] = $adUa;
            $data['city_iso'] = $city_iso;
            $data['country_iso'] = $country_iso;
            $data['continent_iso'] = $continent_iso;
            $data['timezone'] = $timezone;
            $data['utm_source'] = $utm_source; // user
            $data['utm_medium'] = $utm_medium; // user
            $data['utm_content'] = $utm_content; // user
            $data['driver'] = $driver; // user
            $data['triggered_at'] = $riggeredAt;
            $data['created_at'] = date('Y-m-d H:i:s');

            $json = json_encode($data);

            echo $json . PHP_EOL;

//            $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
        }
    }
}
