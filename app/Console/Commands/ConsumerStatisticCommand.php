<?php

namespace App\Console\Commands;

use App\Http\Services\NotifyService;
use App\Subscriber;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Cache;
use Illuminate\Support\Facades\Validator;
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
//                    try {
                        var_dump($message->payload);
                        echo "\n\n";

                        $payload = json_decode($message->payload, true);

                        if ($payload == null) {
                            $this->notifyService->notify("STATISTICS REQUEST PARSE PROBLEM", [], $message->payload);

                            continue;
                        }

                        $adId = $payload['id'];
                        $adUa = $payload['ua'];
                        $adIp = $payload['ip'];
                        $adEvent = $payload['event'];
                        $triggeredAt = $payload['triggered_at'] ?? date('Y-m-d H:i:s');
                        $language = $payload['language'];

                        $dd = new \DeviceDetector\DeviceDetector($adUa);
                        $dd->parse();

                        $_city = $container->get('\GeoIp2\Database\Reader\City');
                        $_country = $container->get('\GeoIp2\Database\Reader\Country');
                        $_connectionType = $container->get('\GeoIp2\Database\Reader\ConnectionType');

                        $_city = $_city->city($adIp);
                        $_country = $_country->country($adIp);
                        $_connectionType = $_connectionType->connectionType($adIp);

                        $user = null;
                        if ($payload['subscriber_id']) {
                            $subscriberId = $payload['subscriber_id'];

                            $user = Subscriber::find($subscriberId);
                        }

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
                        $data['subscriber_id'] = empty($payload['push_subscriber_id']) ? null : $data['push_subscriber_id'];
                        $data['advertiser_id'] = empty($payload['advertiser_id']) ? null : $payload['advertiser_id'];
                        $data['ad_id'] = $adId;
                        $data['publisher_id'] = empty($payload['publisher_id']) ? null : $payload['publisher_id'];
                        $data['website_id'] = empty($payload['push_website_id']) ? null : $payload['push_website_id'];
                        $data['manager_id'] = 'e6b39ed1-56ff-4079-87e0-dc22c7dc4a24'; // Vitalik
                        $data['project_id'] = '9f951b51-a60e-4c74-9346-35c68b66add3'; // RevQuake
                        $data['cost_type'] = empty($payload['type']) ? 'cpc' : $payload['type'];
                        $data['revenue'] = $adEvent == 1 ? round($payload['external_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
                        $data['cost'] = $adEvent == 1 ? round($payload['internal_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
                        $data['profit'] = $adEvent == 1 ? round($payload['external_price'] - $payload['internal_price'], 4, PHP_ROUND_HALF_DOWN) : 0; // redis
                        $data['click'] = $adEvent == 0 ? 0 : 1;
                        $data['impression'] = $adEvent == 1 ? 0 : 1;
                        $data['is_duplicate'] = 0;
                        $data['is_hijack'] = 0;
                        $data['is_fraud'] = 0;
                        $data['is_test'] = 0;
                        $data['ip'] = empty($adIp) ? null : $adIp;
                        $data['os'] = empty($os) ? null : $os;
                        $data['language'] = empty($language) ? null : $language;
                        $data['device'] = empty($device) ? null : $device;
                        $data['brand'] = empty($brand) ? null : $brand;
                        $data['client_type'] = empty($client_type) ? null : $client_type;
                        $data['client_name'] = empty($client_name) ? null : $client_name;
                        $data['connection_type'] = empty($connection_type) ? null : $connection_type;
                        $data['user_agent'] = empty($adUa) ? null : $adUa;
                        $data['city_iso'] = empty($city_iso) ? null : $city_iso;
                        $data['country_iso'] = empty($country_iso) ? null : $country_iso;
                        $data['continent_iso'] = empty($continent_iso) ? null : $continent_iso;
                        $data['timezone'] = empty($timezone) ? null : $timezone;
                        $data['utm_source'] = empty($utm_source) ? null : $utm_source;
                        $data['utm_medium'] = empty($utm_medium) ? null : $utm_medium;
                        $data['utm_content'] = empty($utm_content) ? null : $utm_content;
                        $data['driver'] = empty($driver) ? null : $driver;
                        $data['triggered_at'] = empty($triggeredAt) ? null : $triggeredAt;

                        $data['created_at'] = date('Y-m-d H:i:s');

                        $json = json_encode($data, JSON_PRETTY_PRINT);

                        $bag = $this->validate($data);
                        if ($bag->count() > 0) {
                            $this->notifyService->notify("STATISTICS REQUEST PROBLEM", $bag->getMessages(), $json);
                            continue;
                        }

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

    protected function validate(array $message)
    {
        $validator = Validator::make($message, [
            'id' => 'required|uuid',
            'subscriber_id' => 'nullable|uuid',
            'advertiser_id' => 'nullable|uuid',
            'ad_id' => 'required|uuid',
            'publisher_id' => 'nullable|uuid',
            'website_id' => 'nullable|uuid',
            'manager_id' => 'required|uuid',
            'project_id' => 'required|uuid',
            'cost_type' => 'in:cpc,cpa,cpi',
            'revenue' => 'required|numeric',
            'cost' => 'required|numeric',
            'profit' => 'required|numeric',
            'click' => 'required|in:0,1',
            'impression' => 'required|in:0,1',
            'ip' => 'nullable|ip',
            'os' => 'nullable',
            'language' => 'nullable',
            'device' => 'nullable',
            'brand' => 'nullable',
            'client_type' => 'nullable',
            'client_name' => 'nullable',
            'connection_type' => 'nullable',
            'user_agent' => 'nullable',
            'city_iso' => 'nullable',
            'country_iso' => 'nullable',
            'continent_iso' => 'nullable',
            'timezone' => 'nullable',
            'utm_source' => 'nullable',
            'utm_medium' => 'nullable',
            'utm_content' => 'nullable',
            'driver' => 'nullable',
            'triggered_at' => 'required|date_format:Y-m-d H:i:s',
            'created_at' => 'required|date_format:Y-m-d H:i:s',
        ]);

        return $validator->errors();
    }
}