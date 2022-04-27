<?php

namespace w8io;

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';
require_once __DIR__ . '/include/w8_update_helpers.php';

function selftest()
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    for( $a = 0;; ++$a )
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
        
        $aid = $assetId === 'WAVES' ? 0 : $RO->getIdByAsset( $assetId );
        $info = $RO->getAssetInfoById( $aid );

        $decimals = ( $decimals = $info[0] ) === 'N' ? 0 : (int)$decimals;
        $asset = substr( $info, 2 );

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
            $chainAmount = wk()->balance( $address, $assetId );

            if( $chainAmount !== $amount )
            {
                wk()->log( 'e', ++$e . ') ' . $address . ': ' . w8io_amount( $chainAmount, $decimals ) . ' !== ' . w8io_amount( $amount, $decimals ) );
            }
        }

        wk()->log( 's', $a .': ' . $assetId . ' (' . $asset . ') ' . ( $i - $e ) . ' OK' . ( $e > 0 ? ( ' (' . $e . ' ERROR)' ) : '' ) );
    }
    exit( 'done' );
}

singleton();
$upstats = w8_upstats();

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
            exit( 'rollback needs block number' );
        $block = (int)$argv[2];
        wk()->log( 'w8_rollback to ' . $block );
        rollback( $block );
        wk()->log( 'done' );
        break;
    }
    case 'selftest':
    {
        wk()->setBestNode();
        wk()->log( wk()->getNodeAddress() );
        selftest();
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

            'CREATE INDEX IF NOT EXISTS pts_r2_index ON pts( r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_index ON pts( r3 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_index ON pts( r4 )',
            'CREATE INDEX IF NOT EXISTS pts_r5_index ON pts( r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r10_index ON pts( r10 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r2_index ON pts( r3, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )',
            'CREATE INDEX IF NOT EXISTS pts_r3_r5_index ON pts( r3, r5 )',
            'CREATE INDEX IF NOT EXISTS pts_r4_r5_index ON pts( r4, r5 )',
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
            'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )',
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
    default:
    {
        exit( wk()->log( 'e', 'unknown command:' . $argv[1] ) );
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
            exit( wk()->log( 'e', 'flock failed, already running?' ) );
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

    $blockchain = new Blockchain( W8DB );
    $blockchain->rollback( $block );
}

function updater()
{
    require_once __DIR__ . '/include/Blockchain.php';
    require_once __DIR__ . '/include/BlockchainParser.php';
    require_once __DIR__ . '/include/BlockchainBalances.php';

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

