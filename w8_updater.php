<?php

namespace w8io;

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';
require_once __DIR__ . '/include/w8_update_helpers.php';

if( 0 ) // SELFTEST
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    for( $a = 55524;; ++$a )
    //$a = 0;
    {
        $assetId = $RO->getAssetById( $a );
        if( $assetId === false )
            break;
        $aid = $assetId === 'WAVES' ? 0 : $RO->getIdByAsset( $assetId );
        $info = $RO->getAssetInfoById( $aid );

        $decimals = (int)$info[0];
        $asset = substr( $info, 2 );

        $balances = $RO->db->query( 'SELECT * FROM balances WHERE r2 = ' . $aid );
        $total = 0;
        $i = 0;
        //wk()->log( $a .': ' . $assetId . ' (' . $asset . ')' );
        foreach( $balances as $balance )
        {
            if( ++$i % 10000 === 0 )
                wk()->log( "$i / 24055278" );

            $aid = (int)$balance[1];
            if( $aid <= 0 )
                continue;

            $amount = (int)$balance[3];
            if( $amount < 0 )
                break;

            if( $amount > 0 )
                $amount = $amount;

            $total += $amount;
            $address = $RO->getAddressById( $aid );
            $chainAmount = wk()->balance( $address, $assetId );

            if( $chainAmount !== $amount )
            {
                wk()->log( $a .': ' . $assetId . ' (' . $asset . ')' );
                wk()->log( 'e', $address . ': ' . w8io_amount( $chainAmount, $decimals ) . ' !== ' . w8io_amount( $amount, $decimals ) );
            }
        }
    }
    exit( 'done' );
}

singleton();
if( !isset( $argv[1] ) )
    $argv[1] = 'updater';

switch( $argv[1] )
{
    case 'updater':
    {
        wk()->log( 'w8_updater' );
        wk()->setBestNode();
        wk()->log( wk()->getNodeAddress() );
        updater();
        break;
    }
    case 'indexer':
    {
        wk()->log( 'w8_indexer' );
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

function updater()
{
    require_once __DIR__ . '/include/Blockchain.php';
    require_once __DIR__ . '/include/BlockchainParser.php';
    require_once __DIR__ . '/include/BlockchainBalances.php';

    $blockchain = new Blockchain( W8DB );

    $procs = defined( 'W8IO_UPDATE_PROCS' ) && W8IO_UPDATE_PROCS;
    $sleep = defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17;

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

        sleep( $sleep );
    }
}

