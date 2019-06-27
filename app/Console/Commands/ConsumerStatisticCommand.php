<?php

namespace App\Console\Commands;

use App\Subscriber;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;
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
     * @throws \RdKafka\Exception
     */
    public function handle(\Psr\Container\ContainerInterface $container)
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

        $kafkaConfiguration->set('group.id', env('KAFKA_STATISTICS_TOPIC_QUEUE_GROUP'));
        $kafkaConfiguration->set('metadata.broker.list', env('KAFKA_BROKERS'));

        $topicConfiguration = new \RdKafka\TopicConf();
        $topicConfiguration->set('auto.commit.interval.ms', 100);
        $kafkaConfiguration->setDefaultTopicConf($topicConfiguration);

        $consumer = new \RdKafka\KafkaConsumer($kafkaConfiguration);
        $consumer->subscribe([env('KAFKA_STATISTICS_TOPIC_QUEUE_NAME')]);

        $producer = new \RdKafka\Producer();
        $producer->addBrokers(env('KAFKA_BROKERS'));

        $topic = $producer->newTopic(env('KAFKA_STATISTICS_TOPIC_NAME'));

        while (true) {
            $message = $consumer->consume(120 * 1000);
            switch ($message->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:
                    echo "!!!";

//                    try {
                        $payload = json_decode($message->payload, true);

                        $adId = $payload['Id'];
                        $adUa = $payload['UA'];
                        $adIp = $payload['IP'];
                        $adEvent = $payload['Event'];
                        $riggeredAt = $payload['TriggeredAt'] ?? date('Y-m-d H:i:s');
                        $language = $payload['Language'];

                        $dd = new \DeviceDetector\DeviceDetector($adUa);
                        $dd->parse();

                        $_city = $container->get('\GeoIp2\Database\Reader\City');
                        $_country = $container->get('\GeoIp2\Database\Reader\Country');
                        $_connectionType = $container->get('\GeoIp2\Database\Reader\ConnectionType');

                        $_city = $_city->city($adIp);
                        $_country = $_country->country($adIp);
                        $_connectionType = $_connectionType->connectionType($adIp);

                        $ad = null;

                        if (!$ad) {
                            echo "\nNo ad!\n";
//                            continue;

                            $ad = [];
                            $ad['id'] = Uuid::uuid4()->toString();
                            $ad['raw_id'] = Uuid::uuid4()->toString();
                            $ad['advertiser_id'] = Uuid::uuid4()->toString();
                            $ad['manager_id'] = Uuid::uuid4()->toString();
                            $ad['website_id'] = Uuid::uuid4()->toString();
                            $ad['publisher_id'] = Uuid::uuid4()->toString();
                            $ad['subscriber_id'] = '00003e31-474e-4ed6-ba16-1847801e89e5';
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

                        if ($ad['subscriber_id']) {
                            $subscriberId = $ad['subscriber_id'];
//                            $user = \Illuminate\Support\Facades\Cache::remember( $ad['subscriber_id'], 8600, function () use ($subscriberId) {
//                                return Subscriber::find($subscriberId);
//                            });
                        }

                        $subscriberId = Subscriber::find($subscriberId);

                        $os = $dd->getOs()['name'] ?? null;
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

                        $data = [];
                        $data['id'] = Uuid::uuid4()->toString();
                        $data['subscriber_id'] = $ad['subscriber_id']; // redis
                        $data['advertiser_id'] = $ad['advertiser_id']; // redis
                        $data['ad_id'] = $adId;
                        $data['publisher_id'] = $ad['publisher_id']; // redis
                        $data['website_id'] = $ad['website_id']; // redis
                        $data['manager_id'] = $ad['manager_id']; // redis
                        $data['cost_type'] = $ad['type']; // redis
                        $data['revenue'] = $adEvent == 1 ? round($ad['external_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
                        $data['cost'] = $adEvent == 1 ? round($ad['internal_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
                        $data['profit'] = $adEvent == 1 ? round($ad['external_price'] - $ad['internal_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
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

                        $topic->produce(\RD_KAFKA_PARTITION_UA, 0, $json);
//                    } catch (\Throwable $error) {
//                        echo "\nERROR: " . $error->getMessage() . "\n" . $error->getFile() . "\n";
//                    }

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
