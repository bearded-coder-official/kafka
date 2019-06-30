<?php


namespace App\Http\Services;


class NotifyService
{
    public function notify($header, $messages, $json)
    {
        $lis = [];

        foreach ($messages as $field => $messages) {

            $tmp = array_map(function ($message) {
                return "$message\n";
            }, $messages);

            $lis[] = "<strong>$field:</strong>\n" . implode("\n", $tmp);
        }

        $message = "<b>$header</b>\n\n" . implode("\n", $lis) . "\n" . '<code>' . $json . '</code>';

        file_get_contents('https://api.telegram.org/bot853697990:AAHnAip5WIkpo0fz2Q_6UagbP0EMgGgIP5c/sendMessage?chat_id=-396269443&parse_mode=HTML&text=' . urlencode($message));
    }
}