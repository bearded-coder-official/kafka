<?php

namespace App;

use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Eloquent\SoftDeletes;

final class Subscriber extends Model
{
    use SoftDeletes;

    public $incrementing = false;
}
