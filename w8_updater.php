<?php

namespace w8io;

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';
require_once __DIR__ . '/include/w8_update_helpers.php';

singleton();
wk()->log( 'w8_updater' );
wk()->setBestNode();
wk()->log( wk()->getNodeAddress() );
for( ;; )
{
    updater();
    gc_collect_cycles();
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

    $update_addon = defined( 'W8IO_UPDATE_ADDON' ) && W8IO_UPDATE_ADDON && W8IO_NETWORK === 'W';
    $sleep = defined( 'W8IO_UPDATE_DELAY') ? W8IO_UPDATE_DELAY : 17;

    for( ;; )
    {
        $status = $blockchain->update();

        if( memory_get_usage( true ) / 1024 / 1024 > 4096 )
        {
            wk()->log( 'w', 'restart updater()' );
            sleep( $sleep );
            break;
        }

        if( $status === W8IO_STATUS_UPDATED )
            continue;

        procResetInfo( $blockchain->parser );
        procScam( $blockchain->parser );
        procWeight( $blockchain, $blockchain->parser );

        sleep( $sleep );
    }
}

