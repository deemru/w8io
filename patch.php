<?php

namespace w8io;

if( file_exists( 'config.php' ) )
    require_once 'config.php';
else
    require_once 'config.sample.php';

$db = new \deemru\Triples( W8DB, 'data' );

$patches = $db->query( 'SELECT r0 FROM data WHERE r6 = 0' );

foreach( $patches as $patch )
{
    $db->query( 'UPDATE data SET r2 = 0 WHERE r0 = ' . $patch[0] );
}
