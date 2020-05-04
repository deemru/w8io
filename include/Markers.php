<?php

namespace w8io;

require_once __DIR__ . '/../vendor/autoload.php';
require_once 'KV2.php';

use deemru\KV;

define( 'MARKER_LO', 0 );
define( 'MARKER_HI', 1 );

class Markers
{
    public KV $kv;
    
    public function __construct( $db )
    {
        $this->kv = ( new KV )->setStorage( $db, 'markers', 1, 'INTEGER PRIMARY KEY', 'INTEGER' );
    }

    public function db()
    {
        return $this->kv->db;
    }

    public function setMarkers( $lo, $hi, $forced = false )
    {
        if( isset( $lo ) && ( $forced || $this->getLoMarker() > $lo ) )
            $this->kv->setKeyValue( MARKER_LO, $lo );

        if( isset( $hi ) )
            $this->kv->setKeyValue( MARKER_HI, $hi );

        $this->kv->merge();
    }

    public function getLoMarker()
    {
        $lo = $this->kv->getValueByKey( MARKER_LO );
        return $lo !== false ? $lo : W8IO_TXSHIFT;
    }

    public function getHiMarker()
    {
        $hi = $this->kv->getValueByKey( MARKER_HI );
        return $hi !== false ? $hi : 0;
    }
}
