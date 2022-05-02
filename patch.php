<?php

namespace w8io;

if( file_exists( 'config.php' ) )
    require_once 'config.php';
else
    require_once 'config.sample.php';

$db = new \deemru\Triples( W8DB, 'pts' );

$patches = $db->query( 'SELECT * FROM pts WHERE r2 = 14 UNION SELECT * FROM PTS WHERE r2 = -14' );

foreach( $patches as $patch )
{
    if( $patch[4] !== $patch[5] )
        $db->query( 'UPDATE pts SET r4 = r5 WHERE r0 = ' . $patch[0] );
}
