<?php

namespace App\Providers;

use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     *
     * @return void
     */
    public function register()
    {
        $this->app->singleton('\GeoIp2\Database\Reader\City', function () {
            return new \GeoIp2\Database\Reader(resource_path() . '/maxmind/GeoIP2-City.mmdb');
        });

        $this->app->singleton('\GeoIp2\Database\Reader\Country', function () {
            return new \GeoIp2\Database\Reader(resource_path() . '/maxmind/GeoIP2-Country.mmdb');
        });

        $this->app->singleton('\GeoIp2\Database\Reader\ConnectionType', function () {
            return new \GeoIp2\Database\Reader(resource_path() . '/maxmind/GeoIP2-Connection-Type.mmdb');
        });
    }

    /**
     * Bootstrap any application services.
     *
     * @return void
     */
    public function boot()
    {
        //
    }
}
