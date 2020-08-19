<?php

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';

require_once __DIR__ . '/include/secqru_flock.php';
$lock = new \secqru_flock( W8IO_DB_DIR . 'w8io.lock' );
if( false === $lock->open() )
    exit( wk()->log( 'e', 'flock failed, already running?' ) );

wk()->log( 'w8io_updater started' );

require_once __DIR__ . '/include/w8io_blockchain.php';
$blockchain = new w8io\Blockchain( 'sqlite:' . W8IO_DB_BLOCKCHAIN );
require_once __DIR__ . '/include/w8io_blockchain_transactions.php';
$parser = new w8io\BlockchainParser( $blockchain->db );
require_once __DIR__ . '/include/w8io_blockchain_balances.php';
$balances = new w8io\BlockchainBalances( $blockchain->db );
$blockchain->setParser( $parser );
$blockchain->setBalances( $balances );

function blockchain()
{
    global $blockchain;
    return $blockchain;
}

if( 0 )
{
    $wk = new deemru\WavesKit;
    $wk->setNodeAddress( 'https://api.telegram.org' );
    define( 'WK_CURL_OPTIONS', [ CURLOPT_PROXY => '10.173.6.1:8080' ] );
    $data = $wk->fetch( '/' );
    exit;
}

function update_proc( $blockchain, $transactions, $balances, $aggregate )
{
    $timer = 0;
    //w8io_timer( $timer );

    for( ;; )
    {
        $result = $blockchain->update();
        if( $result === W8IO_STATUS_UPDATED && 0 )
        {
            for( ;; )
            {
                if( $transactions->update() )
                    continue;
                break;

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

        if( $result !== W8IO_STATUS_UPDATED )
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

    $wkt = new WavesKit();
    $wkt->setNodeAddress( 'https://marketdata.wavesplatform.com' );
    $fresh_tickers = $wkt->fetch( '/api/tickers' );
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

if( 0 )
{
    $newblockchain = new w8io\Blockchain( 'C:/w8io-refresh/blockchain.sqlite3' );

    for( ;; )
    {
        $from = $newblockchain->height;
        $q = $blockchain->headers->query( "SELECT * FROM headers WHERE key > $from ORDER BY key ASC LIMIT 1000" );
        $newblockchain->headers->begin();
        {
            foreach( $q as $rec )
                $newblockchain->headers->setKeyValueCache( $rec['key'], $rec['value'], 's' );

            $newblockchain->headers->mergeCache();
        }    
        $newblockchain->headers->commit();
        $newblockchain->setHeight();
        if( $from === $newblockchain->height )
            break;
        wk()->log( $newblockchain->height );
    }

    for( ;; )
    {
        $from = $newblockchain->txheight;
        $q = $blockchain->txids->query( "SELECT * FROM txids WHERE key > $from ORDER BY key ASC LIMIT 100000" );
        $newblockchain->txids->begin();
        {
            foreach( $q as $rec )
                $newblockchain->txids->setKeyValueCache( $rec['key'], $rec['value'], 's' );

            $newblockchain->txids->mergeCache();
        }    
        $newblockchain->txids->commit();
        $newblockchain->setTxHeight();
        if( $from === $newblockchain->txheight )
            break;
        wk()->log( $newblockchain->txheight );
    }

    for( ;; )
    {
        $newblockchain->setTxHeightTxs();
        $from = $newblockchain->txheight;
        $q = $blockchain->txs->query( "SELECT * FROM txs WHERE key > $from ORDER BY key ASC LIMIT 100000" );
        $newblockchain->txs->begin();
        {
            foreach( $q as $rec )
            {
                $value = wk()->json_decode( gzinflate( $rec['value'] ) );
                unset( $value['id'] );
                $newblockchain->txs->setKeyValueCache( $rec['key'], $value, 'jz' );
            }

            $newblockchain->txs->mergeCache();
        }    
        $newblockchain->txs->commit();
        $newblockchain->setTxHeightTxs();
        if( $from === $newblockchain->txheight )
            break;
        wk()->log( $newblockchain->txheight );
    }
    exit;
}
//require_once __DIR__ . '/include/w8io_blockchain_transactions.php';
//$transactions = new w8io_blockchain_transactions();
//require_once __DIR__ . '/include/w8io_blockchain_balances.php';
//$balances = new w8io_blockchain_balances();
//require_once __DIR__ . '/include/w8io_blockchain_aggregate.php';
//$aggregate = new w8io_blockchain_aggregate();

wk()->setBestNode();
wk()->log( 'i', 'setBestNode = ' . wk()->getNodeAddress() );

$update_addon = defined( 'W8IO_UPDATE_ADDON' ) && W8IO_UPDATE_ADDON && W8IO_NETWORK === 'W';
$sleep = defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17;

for( ;; )
{
    update_proc( $blockchain, $parser, null, null );

    if( 0 && $update_addon )
    {
        update_tickers( $transactions );
        update_scam( $transactions );
    }

    sleep( $sleep );
}
