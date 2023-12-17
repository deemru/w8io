<?php

namespace w8io;

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';
require_once __DIR__ . '/include/w8_update_helpers.php';

function selftest( $start, $apikey )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );
    $height = wk()->height() - 1;
    $local_height = $RO->getLastHeightTimestamp()[0];
    if( $local_height < $height )
        w8_err( $local_height . ' < ' . $height . ' (needs update)' );
    if( $local_height !== $height )
        w8_err( $local_height . ' !== ' . $height . ' (needs rollback ' . $height . ')' );

    for( $a = $start;; ++$a )
    {
        if( $a === 0 )
        {
            $assetId = 'WAVES';
        }
        else
        {
            $assetId = $RO->getAssetById( $a );
            if( $assetId === false )
                break;
        }

        $aid = $assetId === 'WAVES' ? WAVES_ASSET : $RO->getIdByAsset( $assetId );
        $info = $RO->getAssetInfoById( $aid );

        $decimals = ( $decimals = $info[0] ) === 'N' ? 0 : (int)$decimals;
        $asset = substr( $info, 2 );

        $node_items = [];
        $after = '';
        $i = 0;
        if( $a === WAVES_ASSET )
        {
            $data = wk()->fetch( '/debug/stateWaves/' . $height, false, null, null, [ "X-API-Key: $apikey" ] );
            $node_items = wk()->json_decode( $data );
        }
        else
        for( ;; )
        {
            $data = wk()->fetch( '/assets/' . $assetId . '/distribution/' . $height . '/limit/1000' . $after );
            $data = wk()->json_decode( $data );
            $node_items = array_merge( $node_items, $data['items'] );
            $i += count( $data['items'] );
            if( $i % 10000 === 0 )
                wk()->log( $a .': ' . $assetId . ' (' . $asset . ') ... ' . $i );
            if( $data['hasNext'] === false )
            {
                if( $i > 10000 )
                    wk()->log( 's', $a .': ' . $assetId . ' (' . $asset . ') ... ' . $i );
                break;
            }
            $lastItem = $data['lastItem'];
            $after = '?after=' . $lastItem;
        }

        $balances = $RO->db->query( 'SELECT * FROM balances WHERE r2 = ' . $aid );
        $i = 0;
        $e = 0;
        foreach( $balances as $balance )
        {
            if( ++$i % 10000 === 0 )
                wk()->log( $a .': ' . $assetId . ' (' . $asset . ') ... ' . $i );

            $aid = (int)$balance[1];
            if( $aid <= 0 )
                continue;

            $amount = (int)$balance[3];

            $address = $RO->getAddressById( $aid );
            $chainAmount = $node_items[$address] ?? 0;

            if( $chainAmount !== $amount )
                wk()->log( 'e', ++$e . ') ' . $address . ': ' . w8io_amount( $chainAmount, $decimals ) . ' !== ' . w8io_amount( $amount, $decimals ) );
            else
                unset( $node_items[$address] );
        }

        foreach( $node_items as $address => $chainAmount )
            wk()->log( 'e', ++$e . ') ' . $address . ': ' . w8io_amount( $chainAmount, $decimals ) . ' !== ' . w8io_amount( 0, $decimals ) );

        wk()->log( 's', $a .': ' . $assetId . ' (' . $asset . ') ' . ( $i - $e ) . ' OK' . ( $e > 0 ? ( ' (' . $e . ' ERROR)' ) : '' ) );
    }
    wk()->log( 'i', 'done' );
}

singleton();
$upstats = w8_upstats();

//$argv[1] = 'rollback';
//$argv[2] = '1060000';

if( !isset( $argv[1] ) )
{
    if( !$upstats['firstrun'] )
        $argv[1] = 'firstrun';
    else if( !$upstats['onbreak'] )
        $argv[1] = 'onbreak';
    else if( !$upstats['updater'] )
        $argv[1] = 'updater';
    else if( !$upstats['indexer'] )
        $argv[1] = 'indexer';
    else
        $argv[1] = 'updater';
    wk()->log( 'run as "' . $argv[1] . '"' );
}

wk()->log( $argv[1] );
switch( $argv[1] )
{
    case 'updater':
    case 'firstrun':
    {
        wk()->setBestNode();
        wk()->log( wk()->getNodeAddress() );
        $upstats['firstrun'] = true;
        w8_upstats( $upstats );
        updater();
        break;
    }
    case 'rollback':
    {
        if( !isset( $argv[2] ) )
            w8_err( 'rollback needs block number' );
        $block = (int)$argv[2];
        wk()->log( 'w8_rollback to ' . $block );
        rollback( $block );
        wk()->log( 'done' );
        break;
    }
    case 'selftest':
    {
        $start = (int)( $argv[2] ?? 0 );
        $apikey = $argv[3] ?? "";
        wk()->setBestNode();
        wk()->log( wk()->getNodeAddress() );
        selftest( $start, $apikey );
        break;
    }
    case 'indexer':
    {
        $db = W8IO_DB_PATH;
        $cmds =
        [
            'CREATE INDEX IF NOT EXISTS balances_r1_index ON balances( r1 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_index ON balances( r2 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_r3_index ON balances( r2, r3 )',
            'CREATE INDEX IF NOT EXISTS balances_r1_r2_index ON balances( r1, r2 )',

            'CREATE INDEX IF NOT EXISTS pts_r2_index ON pts( r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_index ON pts( r3 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_index ON pts( r4 )',
            'CREATE INDEX IF NOT EXISTS pts_r5_index ON pts( r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r10_index ON pts( r10 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r2_index ON pts( r3, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r5_index ON pts( r3, r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r5_index ON pts( r4, r5 )',

            'CREATE INDEX IF NOT EXISTS data_r3_r2_index ON data( r3, r2 )',
            'CREATE INDEX IF NOT EXISTS data_r3_r4_index ON data( r3, r4 )',
        ];

        foreach( $cmds as $cmd )
        {
            $tt = microtime( true );
            $cmd = 'sqlite3 ' . W8IO_DB_PATH . ' "' . $cmd . '"';
            wk()->log( 'exec( ' . $cmd . ' )' );
            exec( $cmd );
            wk()->log( sprintf( '%.00f seconds', ( microtime( true ) - $tt ) ) );
        }
        wk()->log( 'done' );
        $upstats['indexer'] = true;
        w8_upstats( $upstats );
        break;
    }
    case 'onbreak':
    {
        $db = W8IO_DB_PATH;
        $cmds =
        [
            'CREATE INDEX IF NOT EXISTS balances_r1_r2_index ON balances( r1, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )',
            'CREATE INDEX IF NOT EXISTS data_r3_r4_index ON data( r3, r4 )',
        ];

        foreach( $cmds as $cmd )
        {
            $tt = microtime( true );
            $cmd = 'sqlite3 ' . W8IO_DB_PATH . ' "' . $cmd . '"';
            wk()->log( 'exec( ' . $cmd . ' )' );
            exec( $cmd );
            wk()->log( sprintf( '%.00f seconds', ( microtime( true ) - $tt ) ) );
        }
        wk()->log( 'done' );
        $upstats['onbreak'] = true;
        w8_upstats( $upstats );
        break;
    }
    case 'wipe':
    {
        if( file_exists( W8IO_DB_PATH ) )
        {
            wk()->log( 'w', W8IO_DB_PATH );
            wk()->log( 'w', 'DATABASE WILL BE DESTROYED after 10 seconds...' );
            sleep( 10 );
        }

        $db = W8IO_DB_PATH;
        $db_shm = "$db-shm";
        $db_wal = "$db-wal";
        $files = [ $db, $db_shm, $db_wal,
            W8IO_DB_DIR . 'upstats.json',
            W8IO_DB_DIR . 'weights.txt',
            W8IO_DB_DIR . 'scams.txt',
        ];

        foreach( $files as $file )
        {
            if( file_exists( $file ) )
            {
                $result = unlink( $file );
                wk()->log( $result ? 's' : 'e', "$file was " . ( $result ? '' : 'NOT ' ) . 'deleted' );
            }
            else
            {
                wk()->log( "$file not exists" );
            }
        }
        break;
    }
    default:
    {
        w8_err( 'unknown command:' . $argv[1] );
    }
}

function singleton()
{
    require_once __DIR__ . '/include/secqru_flock.php';
    static $singleton;
    if( !isset( $singleton ) )
    {
        $singleton = new \secqru_flock( W8IO_DB_DIR . 'w8io.lock' );
        if( false === $singleton->open() )
            w8_err( 'flock failed, already running?' );
        return true;
    }
    return false;
}

function w8_upstats( $upstats = null )
{
    $upstats_file = W8IO_DB_DIR . 'upstats.json';
    if( !file_exists( $upstats_file ) || isset( $upstats ) )
    {
        if( !isset( $upstats ) )
            $upstats =
            [
                'firstrun' => false,
                'onbreak' => false,
                'indexer' => false,
                'updater' => false,
            ];

        file_put_contents( $upstats_file, json_encode( $upstats, JSON_PRETTY_PRINT ) );
    }

    return wk()->json_decode( file_get_contents( $upstats_file ) );
}

function rollback( $block )
{
    require_once __DIR__ . '/include/Blockchain.php';
    require_once __DIR__ . '/include/BlockchainParser.php';
    require_once __DIR__ . '/include/BlockchainBalances.php';
    require_once __DIR__ . '/include/BlockchainData.php';

    $blockchain = new Blockchain( W8DB );
    $blockchain->rollback( $block + 1 );
}

function updater()
{
    require_once __DIR__ . '/include/Blockchain.php';
    require_once __DIR__ . '/include/BlockchainParser.php';
    require_once __DIR__ . '/include/BlockchainBalances.php';
    require_once __DIR__ . '/include/BlockchainData.php';

    $blockchain = new Blockchain( W8DB );

    $procs = defined( 'W8IO_UPDATE_PROCS' ) && W8IO_UPDATE_PROCS;
    $sleep = defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17;
    $break = w8_upstats()['updater'] === false;

    for( ;; )
    {
        $status = $blockchain->update();

        if( defined( W8IO_MAX_MEMORY ) && memory_get_usage( true ) > W8IO_MAX_MEMORY )
        {
            wk()->log( 'w', 'restart updater()' );
            sleep( $sleep );
            break;
        }

        if( $status === W8IO_STATUS_UPDATED )
            continue;

        if( $procs )
        {
            procResetInfo( $blockchain->parser );
            if( W8IO_NETWORK === 'W' )
                procScam( $blockchain->parser );
            procWeight( $blockchain, $blockchain->parser );
        }

        if( $break )
        {
            $upstats = w8_upstats();
            $upstats['updater'] = true;
            w8_upstats( $upstats );
            wk()->log( 's', 'first updater done' );
            break;
        }

        sleep( $sleep );
    }
}
