<?php

require_once 'w8io_config.php';
require_once './include/w8io_nodes.php';
require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';
require_once './include/w8io_api.php';

$api = new w8io_api();

$wtxs = $api->get_address_transactions( 'awes', 1035092, 100 );

foreach( $wtxs as $wtx )
{
    var_dump( $wtx );
}