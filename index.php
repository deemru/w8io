<?php


require_once 'w8io_config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = '3PAWwWa6GbwcJaFzwqXQN5KQm7H96Y7SHTQ';

$address = explode( '/', $uri )[0];

if( empty( $address ) )
    $address = 'GENESIS';
    
require_once './include/w8io_nodes.php';
require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';
require_once './include/w8io_api.php';

$api = new w8io_api();

$balance = $api->get_address_balance( $address );

if( $balance === false )
    w8io_error( "get_address_balance( $address ) failed" );

echo '<pre>';

echo "address = $address" . PHP_EOL;
echo "height = {$balance['height']}" . PHP_EOL . PHP_EOL;

foreach( $balance['balance'] as $asset => $amount )
{
    if( !$asset )
        echo "Waves = $amount" . PHP_EOL;
    else
        echo "$asset = $amount" . PHP_EOL;
}

echo PHP_EOL . memory_get_peak_usage() / 1024 / 1024;
echo PHP_EOL . 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] );
echo '</pre>';
