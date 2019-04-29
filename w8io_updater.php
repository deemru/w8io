<?php

require_once 'w8io_config.php';
require_once './include/w8io_nodes.php';
require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';
require_once './include/w8io_blockchain_aggregate.php';

function w8io_lock()
{
    require_once './third_party/secqru/include/secqru_flock.php';
    $lock = new secqru_flock( W8IO_DB_DIR . 'w8io.lock' );
    if( false === $lock->open() )
        return false;
    return $lock;
}

while( false === ( $lock = w8io_lock() ) )
    w8io_trace( 'w', 'w8io_lock() failed' );

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
                $transactions_from_to = $transactions->update( $blockchain_from_to );
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
    $tickers_file = './var/tickers.txt';

    if( file_exists( $tickers_file ) )
    {
        if( time() - filemtime( $tickers_file ) < 7200 )
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
        else
        {
            $temp = [];
            foreach( $last_tickers as $ticker )
            {
                if( $ticker['24h_volume'] > 0 )
                {
                    $temp[] = $ticker['amountAssetID'];
                    $temp[] = $ticker['priceAssetID'];
                }
            }

            $last_tickers = array_unique( $temp );
        }
    }
    else
        $last_tickers = [];

    $fresh_tickers = new w8io_nodes( [ 'marketdata.wavesplatform.com' ] );
    $fresh_tickers = $fresh_tickers->get( '/api/tickers' );
    if( $fresh_tickers === false )
    {
        w8io_trace( 'w', 'fresh_tickers->get() failed' );
        return;
    }

    $tickers = json_decode( $fresh_tickers, true, 512, JSON_BIGINT_AS_STRING );
    if( !is_array( $tickers ) )
    {
        w8io_trace( 'w', 'json_decode( fresh_tickers ) failed' );
        return;
    }

    $temp = [];
    foreach( $tickers as $ticker )
    {
        if( $ticker['24h_volume'] > 0 )
        {
            $temp[] = $ticker['amountAssetID'];
            $temp[] = $ticker['priceAssetID'];
        }
    }
    $tickers = array_unique( $temp );

    $mark_tickers = array_diff( $tickers, $last_tickers );
    $unset_tickers = array_diff( $last_tickers, $tickers );

    foreach( $mark_tickers as $ticker )
        $transactions->mark_tickers( $ticker, true );

    foreach( $unset_tickers as $ticker )
        $transactions->mark_tickers( $ticker, false );

    file_put_contents( $tickers_file, $fresh_tickers );
}

function update_scam( $transactions )
{
    $scam_file = './var/scam.txt';

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

    $fresh_scam = new w8io_nodes( [ 'raw.githubusercontent.com' ] );
    $fresh_scam = $fresh_scam->get( '/wavesplatform/waves-community/master/Scam%20tokens%20according%20to%20the%20opinion%20of%20Waves%20Community.csv' );
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

for( ;; )
{
    update_proc( $blockchain, $transactions, $balances, $aggregate );

    if( W8IO_NETWORK === 'W' )
    {
        update_tickers( $transactions );
        update_scam( $transactions );
    }

    sleep( defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17 );
}
