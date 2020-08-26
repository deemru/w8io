<?php

namespace w8io;

use deemru\Triples;

class RO
{
    public Triples $db;

    public function __construct( $db )
    {
        $this->db = new Triples( $db, 'pts' );
    }

    public function getTxKeyById( $txid )
    {
        $txid = d58( $txid );
        $bucket = unpack( 'J1', $txid )[1];
        $txpart = substr( $txid, 8 );

        if( !isset( $this->q_getTxKeyById ) )
        {
            $this->q_getTxKeyById = $this->db->db->prepare( 'SELECT r0 FROM ts WHERE r1 == ? AND r2 == ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->q_getTxKeyById === false )
                w8_err();
        }

        if( false === $this->q_getTxKeyById->execute( [ $bucket, $txpart ] ) )
            w8_err();

        $r = $this->q_getTxKeyById->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }
}
