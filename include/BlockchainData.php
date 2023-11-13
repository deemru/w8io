<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

class BlockchainData
{
    private Triples $data;
    private $actives;
    private KV $kvKeys;
    private KV $kvValues;
    private $kvs;

    private Triples $db;
    private $uid;
    private $empty;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->data = new Triples( $this->db, 'data', 1,
            // uid                 | txkey    | active   | address  | key      | value    | type
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         0,         0,         0,         0,         0 ] );

/*
        $indexer =
        [
            'CREATE INDEX IF NOT EXISTS data_r3_r2_index ON data( r3, r2 )',
            'CREATE INDEX IF NOT EXISTS data_r3_r4_index ON data( r3, r4 )',
        ];
*/

        $this->kvKeys = ( new KV( true ) )->setStorage( $this->db, 'datakeys', true );
        $this->kvValues = ( new KV( true ) )->setStorage( $this->db, 'datavalues', true );

        $this->kvs = [
            $this->kvKeys,
            $this->kvValues,
        ];

        $this->actives = [];
        $this->setUid();
        $this->empty = $this->uid === 0;
    }

    public function cacheHalving()
    {
        $this->actives = [];
        foreach( $this->kvs as $kv )
            $kv->cacheHalving();
    }

    //$this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );

    private $q_uids;

    public function rollback( $txfrom )
    {
        if( !isset( $this->q_uids ) )
        {
            $this->q_uids = $this->data->db->prepare( 'SELECT r3, r4 FROM data WHERE r1 >= ?' );
            if( $this->q_uids === false )
                w8_err( 'rollback' );
        }

        if( false === $this->q_uids->execute( [ $txfrom ] ) )
            w8_err( 'rollback' );

        $akeys = $this->q_uids->fetchAll();

        if( count( $akeys ) )
        {
            $this->data->query( 'DELETE FROM data WHERE r1 >= '. $txfrom );
            $this->setUid();
            $this->actives = [];

            $updated = [];
            foreach( $akeys as [ $address, $key ] )
            {
                if( !isset( $updated[$address][$key] ) )
                {
                    $updated[$address][$key] = true;
                    $luid = $this->getLastUid( $address, $key );
                    if( $luid !== 0 )
                        $this->updateData( $luid, 1 );
                }
            }
        }
    }

    private function setUid()
    {
        if( false === ( $this->uid = $this->data->getHigh( 0 ) ) )
            $this->uid = 0;
    }

    private function getNewUid()
    {
        return ++$this->uid;
    }

    private $q_getLastUid;

    private function getLastUid( $address, $key )
    {
        if( !isset( $this->q_getLastUid ) )
        {
            $this->q_getLastUid = $this->data->db->prepare( 'SELECT r0 FROM data WHERE r3 = ? AND r4 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->q_getLastUid === false )
                w8_err( 'getLastUid' );
        }

        if( false === $this->q_getLastUid->execute( [ $address, $key ] ) )
            w8_err( 'getLastUid' );

        $luid = $this->q_getLastUid->fetchAll();
        return $luid[0][0] ?? 0;
    }

    private function setNewUid( $address, $key, $uid ) // set and return last uid
    {
        $luid = $this->actives[$address][$key] ?? false;
        if( $luid === false )
        {
            if( $this->empty )
                $luid = 0;
            else
                $luid = $this->getLastUid( $address, $key );
        }

        $this->actives[$address][$key] = $uid;
        return $luid;
    }

    private $q_insertData;

    private function insertData( $uid, $txkey, $active, $address, $key, $value, $type )
    {
        if( !isset( $this->q_insertData ) )
        {
            $this->q_insertData = $this->data->db->prepare( 'INSERT INTO data( r0, r1, r2, r3, r4, r5, r6 ) VALUES( ?, ?, ?, ?, ?, ?, ? )' );
            if( $this->q_insertData === false )
                w8_err( 'insertBalance' );
        }

        if( false === $this->q_insertData->execute( [ $uid, $txkey, $active, $address, $key, $value, $type ] ) )
            w8_err( 'insertBalance' );
    }

    private $q_updateData;

    private function updateData( $uid, $active )
    {
        if( !isset( $this->q_updateData ) )
        {
            // update only types != TYPE_NULL
            $this->q_updateData = $this->data->db->prepare( 'UPDATE data SET r2 = ? WHERE r0 = ? AND r6 != 0' );
            if( $this->q_updateData === false )
                w8_err( 'updateData' );
        }

        if( false === $this->q_updateData->execute( [ $active, $uid ] ) )
            w8_err( 'updateData' );
    }

    public function update( $datarecs )
    {
        foreach( $datarecs as $datarec )
        {
            $data = $datarec[2];

            $uid = $this->getNewUid();
            $txkey = $datarec[0];
            $address = $datarec[1];
            $key = $this->kvKeys->getForcedKeyByValue( $data['key'] );
            $value = $data['value'];
            if( $value === null )
            {
                $type = TYPE_NULL;
                $value = 0;
                $active = 0;
            }
            else
            {
                $type = $data['type'];
                $value = $data['value'];
                $active = 1;

                if( $type === 'string' )
                {
                    $type = TYPE_STRING;
                    $value = $this->kvValues->getForcedKeyByValue( $value );
                }
                else
                if( $type === 'integer' )
                {
                    $type = TYPE_INTEGER;
                    //$value = $value;
                }
                else
                if( $type === 'boolean' )
                {
                    $type = TYPE_BOOLEAN;
                    $value = $value ? 1 : 0;
                }
                else
                if( $type === 'binary' )
                {
                    $type = TYPE_BINARY;
                    $value = base64_decode( substr( $value, 7 ) );
                    if( $value === false )
                        w8_err( 'base64_decode' );
                    $value = $this->kvValues->getForcedKeyByValue( $value );
                }
                else
                    w8_err( 'unknown type: ' . $type );
            }

            $luid = $this->setNewUid( $address, $key, $uid );

            if( $luid !== 0 )
                $this->updateData( $luid, 0 );

            $this->insertData( $uid, $txkey, $active, $address, $key, $value, $type );
        }

        foreach( $this->kvs as $kv )
            $kv->merge();
    }
}
