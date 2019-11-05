<?php

require_once __DIR__ . '/include/error_handler.php';
require_once __DIR__ . '/vendor/autoload.php';

use deemru\WavesKit;
use deemru\Pairs;

if( file_exists( 'w8io_config.php' ) ) 
    require_once 'w8io_config.php';
else
    require_once 'w8io_config.sample.php';
require_once './include/w8io_base.php';
require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';
require_once './include/w8io_blockchain_aggregate.php';
require_once './include/w8io_api.php';

require_once __DIR__ . '/include/secqru_flock.php';
$lock = new \secqru_flock( W8IO_DB_DIR . 'w8io.lock' );
if( false === $lock->open() )
    exit( wk()->log( 'e', 'flock failed, already running?' ) );

w8io_trace( 'i', 'w8io_updater started' );

function update_proc( $blockchain, $transactions, $balances, $aggregate )
{
    $timer = 0;
    w8io_timer( $timer );

    for( ;; )
    {
        $blockchain_from_to = $blockchain->update();
        if( is_array( $blockchain_from_to ) )
        {
            for( ;; )
            {
                $transactions_from_to = $transactions->update( $blockchain_from_to, $balances );
                if( !is_array( $transactions_from_to ) )
                    w8io_error( 'unexpected update transactions error' );

                $blockchain_from_to['from'] = $transactions_from_to['to'];

                $transactions_from = $transactions_from_to['from'];
                for( ;; )
                {
                    $balances_from_to = $balances->update( $transactions_from_to );
                    if( !is_array( $balances_from_to ) )
                        w8io_error( 'unexpected update balances error' );

                    // selfcheck: 100M WAVES
                    if( 0 )
                    {
                        $waves = $balances->get_all_waves();
                        $waves += $transactions->get_hang_waves( $balances_from_to['to'] + 1 );
                        if( $waves !== 10000000000000000 )
                            w8io_error( $waves . ' != 10000000000000000' );
                        w8io_trace( 's', $waves );
                    }

                    if( $transactions_from_to['to'] <= $balances_from_to['to'] )
                        break;

                    $transactions_from_to['from'] = $balances_from_to['to'];
                }

                $transactions_from_to['from'] = $transactions_from;
                for( ;; )
                {
                    $aggregate_from_to = $aggregate->update( $transactions_from_to );
                    if( !is_array( $aggregate_from_to ) )
                        w8io_error( 'unexpected update balances error' );

                    if( $transactions_from_to['to'] <= $aggregate_from_to['to'] )
                        break;

                    $transactions_from_to['from'] = $aggregate_from_to['to'];
                }

                if( $blockchain_from_to['to'] <= $transactions_from_to['to'] )
                    break;
            }
        }

        if( $blockchain_from_to['height'] <= $blockchain_from_to['to'] )
            break;
    }

    if( isset( $balances_from_to ) )
        w8io_trace( 's', "{$balances_from_to['from']} >> {$balances_from_to['to']} (" . w8io_ms( w8io_timer( $timer ) ) . ' ms)' );
}

function update_tickers( $transactions )
{
    $tickers_file = W8IO_DB_DIR . 'tickers.txt';

    if( file_exists( $tickers_file ) )
    {
        if( time() - filemtime( $tickers_file ) < 3600 )
            return;

        $last_tickers = file_get_contents( $tickers_file );
        if( $last_tickers === false )
        {
            w8io_warning( 'file_get_contents() failed' );
            return;
        }

        $last_tickers = json_decode( $last_tickers, true, 512, JSON_BIGINT_AS_STRING );
        if( $last_tickers === false )
            $last_tickers = [];
    }
    else
        $last_tickers = [];

    $height = $transactions->get_height() - 1440;
    $exchanges = $transactions->query( 'SELECT type, asset FROM transactions WHERE block > ' . $height );

    $temp = [];
    foreach( $exchanges as $tx )
    {
        if( $tx[0] !== '7' )
            continue;
        $asset = (int)$tx[1];
        if( $asset > 0 )
            $temp[] = $asset;
    }

    $api = new w8io_api();
    $temp = array_unique( $temp );

    $tickers[] = "WAVES";
    foreach( $temp as $rec )
        $tickers[] = $api->get_asset_id( $rec );

    $mark_tickers = array_diff( $tickers, $last_tickers );
    $unset_tickers = array_diff( $last_tickers, $tickers );

    foreach( $mark_tickers as $ticker )
        $transactions->mark_tickers( $ticker, true );

    foreach( $unset_tickers as $ticker )
        $transactions->mark_tickers( $ticker, false );

    file_put_contents( $tickers_file, json_encode( $tickers ) );
}

function update_scam( $transactions )
{
    $scam_file = W8IO_DB_DIR . 'scam.txt';

    if( file_exists( $scam_file ) )
    {
        if( time() - filemtime( $scam_file ) < 3600 )
            return;

        $last_scam = file_get_contents( $scam_file );
        if( $last_scam === false )
        {
            w8io_warning( 'file_get_contents() failed' );
            return;
        }

        $last_scam = explode( "\n", $last_scam );
    }
    else
        $last_scam = [];

    $wks = new WavesKit();
    $wks->setNodeAddress( 'https://raw.githubusercontent.com' );
    $fresh_scam = $wks->fetch( '/wavesplatform/waves-community/master/Scam%20tokens%20according%20to%20the%20opinion%20of%20Waves%20Community.csv' );
    if( $fresh_scam === false )
    {
        w8io_warning( 'fresh_scam->get() failed' );
        return;
    }

    $scam = explode( "\n", $fresh_scam );
    $scam = array_unique( $scam );
    $fresh_scam = implode( "\n", $scam );

    $mark_scam = array_diff( $scam, $last_scam );
    $unset_scam = array_diff( $last_scam, $scam );

    foreach( $mark_scam as $scamid )
        if( !empty( $scamid ) )
            $transactions->mark_scam( $scamid, true );

    foreach( $unset_scam as $scamid )
        if( !empty( $scamid ) )
            $transactions->mark_scam( $scamid, false );

    file_put_contents( $scam_file, $fresh_scam );
}

$blockchain = new w8io_blockchain();
$transactions = new w8io_blockchain_transactions();
$balances = new w8io_blockchain_balances();
$aggregate = new w8io_blockchain_aggregate();

wk()->setBestNode();
wk()->log( 'i', "setBestNode = " . wk()->getNodeAddress() );

$update_addon = defined( 'W8IO_UPDATE_ADDON' ) && W8IO_UPDATE_ADDON && W8IO_NETWORK === 'W';
$sleep = defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17;

for( ;; )
{
    update_proc( $blockchain, $transactions, $balances, $aggregate );

    if( $update_addon )
    {
        update_tickers( $transactions );
        update_scam( $transactions );
    }

    sleep( $sleep );
}
