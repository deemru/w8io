<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

class BlockchainData
{
    private Triples $data;
    private KV $actives;
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
            // uid                 | txkey    | active   | address  | key      | value    | type     | puid
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         0,         0,         0,         0,         0,         0 ] );

        // INDEX
        //$this->data->db->exec( 'CREATE INDEX IF NOT EXISTS data_r2_r3_index ON data( r2, r3 )' );
        //$this->data->db->exec( 'CREATE INDEX IF NOT EXISTS data_r3_r4_index ON data( r3, r4 )' );

        $this->kvKeys = ( new KV( true ) )->setStorage( $this->db, 'datakeys', true );
        $this->kvValues = ( new KV( true ) )->setStorage( $this->db, 'datavalues', true );

        $this->kvs = [
            $this->kvKeys,
            $this->kvValues,
        ];

        $this->actives = new KV;
        $this->setUid();
        $this->empty = $this->uid === 0;
    }

    public function cacheHalving()
    {
        $this->actives->cacheHalving();
        foreach( $this->kvs as $kv )
            $kv->cacheHalving();
    }

    //$this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );

    private $q_uids;

    public function rollback( $txfrom )
    {
        if( !isset( $this->q_uids ) )
        {
            $this->q_uids = $this->data->db->prepare( 'SELECT r7 FROM data WHERE r1 >= ?' );
            if( $this->q_uids === false )
                w8io_error( 'rollback' );
        }

        if( false === $this->q_uids->execute( [ $txfrom ] ) )
            w8io_error( 'rollback' );

        $puids = $this->q_uids->fetchAll();

        if( count( $puids ) )
        {
            $this->data->query( 'DELETE FROM data WHERE r1 >= '. $txfrom );
            $this->setUid();
            $this->actives->reset();

            foreach( $puids as [ $puid ] )
            {
                if( $puid !== 0 && $puid <= $this->uid )
                    $this->updateData( $puid, 1 );
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

    private function setNewUid( $address, $datakey, $uid )
    {
        $key = $address . '_' . $datakey;
        $puid = $this->actives->getValueByKey( $key );
        if( $puid === false )
        {
            if( $this->empty )
                $puid = 0;
            else
            {
                if( !isset( $this->q_getLastUid ) )
                {
                    $this->q_getLastUid = $this->data->db->prepare( 'SELECT r0 FROM data WHERE r3 = ? AND r4 = ? ORDER BY r0 DESC LIMIT 1' );
                    if( $this->q_getLastUid === false )
                        w8io_error( 'setNewUid' );
                }

                if( false === $this->q_getLastUid->execute( [ $address, $datakey ] ) )
                    w8io_error( 'setNewUid' );

                $puid = $this->q_getLastUid->fetchAll();
                if( isset( $puid[0] ) )
                    $puid = (int)$puid[0][0];
                else
                    $puid = 0;
            }
        }

        $this->actives->setKeyValue( $key, $uid );
        return $puid;
    }

    private $q_insertData;

    private function insertData( $uid, $txkey, $active, $address, $key, $value, $type, $puid )
    {
        if( !isset( $this->q_insertData ) )
        {
            $this->q_insertData = $this->data->db->prepare( 'INSERT INTO data( r0, r1, r2, r3, r4, r5, r6, r7 ) VALUES( ?, ?, ?, ?, ?, ?, ?, ? )' );
            if( $this->q_insertData === false )
                w8io_error( 'insertBalance' );
        }

        if( false === $this->q_insertData->execute( [ $uid, $txkey, $active, $address, $key, $value, $type, $puid ] ) )
            w8io_error( 'insertBalance' );
    }

    private $q_updateData;

    private function updateData( $uid, $active )
    {
        if( !isset( $this->q_updateData ) )
        {
            $this->q_updateData = $this->data->db->prepare( 'UPDATE data SET r2 = ? WHERE r0 = ?' );
            if( $this->q_updateData === false )
                w8io_error( 'updateData' );
        }

        if( false === $this->q_updateData->execute( [ $active, $uid ] ) )
            w8io_error( 'updateData' );
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
                $value = 0;
                $type = TYPE_NULL;
            }
            else
            {
                $type = $data['type'];
                $value = $data['value'];

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
                        w8io_error( 'base64_decode' );
                    $value = $this->kvValues->getForcedKeyByValue( $value );
                }
                else
                    w8io_error( 'unknown type: ' . $type );
            }

            $puid = $this->setNewUid( $address, $key, $uid );

            if( $puid !== 0 )
                $this->updateData( $puid, 0 );

            $this->insertData( $uid, $txkey, 1, $address, $key, $value, $type, $puid );
        }

        foreach( $this->kvs as $kv )
            $kv->merge();
    }
}
