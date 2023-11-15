<?php

namespace w8io;

if( file_exists( __DIR__ . '/config.php' ) )
    require_once __DIR__ . '/config.php';
else
    require_once __DIR__ . '/config.sample.php';
require_once __DIR__ . '/include/common.php';

use deemru\Triples;
use deemru\KV;

function offline( int $delay = 3 )
{
    if( $delay < 1 )
        $delay = 1;
    wk()->log( 'w', 'OFFLINE: delay for ' . $delay . ' sec...' );
    sleep( $delay );
}

function blockUnique( $headers )
{
    return $headers['id'] ?? $headers['signature'];
}

function singleton()
{
    require_once __DIR__ . '/include/secqru_flock.php';
    static $singleton;
    if( !isset( $singleton ) )
    {
        $singleton = new \secqru_flock( W8IO_DB_DIR . 'w8io_headers.lock' );
        if( false === $singleton->open() )
            w8_err( 'flock failed, already running?' );
        return true;
    }
    return false;
}

singleton();
$wk = wk();
$db = new Triples( W8IO_DB_DIR . 'headers.sqlite3', 'headers', true );
$kvHeaders = ( new KV )->setStorage( $db, 'headers', true, 'INTEGER PRIMARY KEY', 'TEXT' )->setValueAdapter( function( $value ){ return json_unpack( $value ); }, function( $value ){ return json_pack( $value ); } );

$hi = $db->getHigh( 0 );
if( $hi === false )
for( ;; )
{
    $headers = $wk->getBlockAt( 1, true );
    if( $headers === false )
    {
        offline();
        continue;
    }
    $kvHeaders->setKeyValue( 1, $headers );
    $kvHeaders->merge();
    $wk->log( 1 );
    break;
}

function rollback()
{
    global $wk;
    global $db;
    global $kvHeaders;

    $hi = $db->getHigh( 0 );
    for( $i = $hi; $i >= 1; --$i )
    {
        $checkpoint = $kvHeaders->getValueByKey( $i );
        for( ;; )
        {
            $headers = $wk->getBlockAt( $i, true );
            if( $headers !== false )
                break;
            offline();
        }

        if( blockUnique( $checkpoint ) === blockUnique( $headers ) )
            break;
        $wk->log( 'w', 'fork @ '. $i );
    }

    if( $i !== $hi )
    {
        $kvHeaders->reset();
        $db->query( 'DELETE from headers WHERE r0 > ' . $i );
        $hi = $i;
        if( $hi === 0 )
            w8_err( 'fork @ GENESIS' );
    }

    return $hi;
}

for( ;; )
{
    $wk->log( '.' );
    $height = $wk->height();
    if( $height === false )
    {
        offline( $delay );
        continue;
    }

    $hi = $db->getHigh( 0 );
    for( $i = $hi + 1; $i < $height; ++$i )
    {
        for( ;; )
        {
            $headers = $wk->getBlockAt( $i, true );
            if( $headers !== false )
                break;
            offline();
        }
        $checkpoint = $kvHeaders->getValueByKey( $i - 1 );
        if( $headers['reference'] !== blockUnique( $checkpoint ) )
        {
            $i = rollback();
            break;
        }
        $kvHeaders->reset();
        $kvHeaders->setKeyValue( $i, $headers );
        $kvHeaders->merge();
        $wk->log( $i );
    }

    if( $i >= $height )
        sleep( 17 );
}
