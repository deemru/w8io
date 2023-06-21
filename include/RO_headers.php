<?php

namespace w8io;

require_once 'common.php';

use deemru\KV;
use deemru\Triples;

class RO_headers
{
    public Triples $db;
    public KV $kv;

    public function __construct()
    {
        $this->db = new Triples( W8IO_DB_DIR . 'headers.sqlite3', 'headers' );
        $this->kv = ( new KV )->setStorage( $this->db, 'headers', false, 'INTEGER PRIMARY KEY', 'TEXT' )->setValueAdapter( function( $value ){ return json_unpack( $value ); }, function( $value ){ return json_pack( $value ); } );
    }
}
