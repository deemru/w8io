<?php

namespace w8io;

if( file_exists( 'config.php' ) )
    require_once 'config.php';
else
    require_once 'config.sample.php';

if( 0 )
{
    $db = new \deemru\Triples( W8DB, 'data' );
    $patches = $db->query( 'SELECT r0 FROM data WHERE r6 = 0' );
    foreach( $patches as $patch )
    {
        $db->query( 'UPDATE data SET r2 = 0 WHERE r0 = ' . $patch[0] );
    }
}

if( 10 )
{
    $cmds =
    [
        'ALTER TABLE data DROP COLUMN r7',
    ];

    foreach( $cmds as $cmd )
    {
        $tt = microtime( true );
        $cmd = 'sqlite3 ' . W8IO_DB_PATH . ' "' . $cmd . '"';
        wk()->log( 'exec( ' . $cmd . ' )' );
        exec( $cmd );
        wk()->log( sprintf( '%.00f seconds', ( microtime( true ) - $tt ) ) );
    }
}
