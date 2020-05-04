<?php

namespace deemru;

require_once __DIR__ . '/../vendor/autoload.php';

class KV
{
    public Triples $db;

    public function __construct( $bidirectional = false )
    {
        $this->kv = [];
        if( $bidirectional )
            $this->vk = [];
        $this->hits = [];
    }

    public function reset()
    {
        $this->merge();

        $this->kv = [];
        if( isset( $this->vk ) )
            $this->vk = [];
        $this->hits = [];
    }

    public function setStorage( &$db, $name, $writable, $keyType = 'INTEGER PRIMARY KEY', $valueType = 'TEXT UNIQUE' )
    {
        $this->db = new Triples( $db, $name, $writable, [ $keyType, $valueType ] );
        $this->ki = false !== strpos( $keyType, 'INTEGER' );
        $this->vi = false !== strpos( $valueType, 'INTEGER' );

        if( $writable )
            $this->recs = [];

        return $this;
    }

    public function setHigh()
    {
        if( false === ( $this->high = $this->db->getHigh( 0 ) ) )
            $this->high = 0;
    }

    public function __destruct()
    {
        $this->merge();
    }

    public function merge()
    {
        if( isset( $this->recs ) && count( $this->recs ) )
        {
            $this->db->merge( $this->recs, true );
            $this->recs = [];
        }
    }

    public function setKeyValue( $key, $value )
    {
        if( isset( $this->vk ) && isset( $this->kv[$key] ) )
            $this->vk[$this->kv[$key]] = false;

        $this->kv[$key] = $value;
        if( isset( $this->vk ) )
            $this->vk[$value] = $key;
        $this->hits[$key] = 1;
        
        if( isset( $this->recs ) )
            $this->recs[$key] = $value;

        return $key;
    }

    public function getKeyByValue( $value )
    {
        assert( isset( $this->vk ) );

        if( isset( $this->vk[$value] ) )
        {
            $key = $this->vk[$value];
            ++$this->hits[$key];
            return $key;
        }

        if( isset( $this->db ) )
        {
            $key = $this->db->getUno( 1, $value );
            if( $key !== false )
                $key = $this->ki ? (int)$key[0] : $key[0];
        }
        else
            $key = false;
        
        $this->kv[$key] = $value;
        $this->vk[$value] = $key;
        $this->hits[$key] = 1;

        return $key;
    }

    public function getValueByKey( $key )
    {
        if( isset( $this->kv[$key] ) )
        {
            ++$this->hits[$key];
            return $this->kv[$key];
        }

        if( isset( $this->db ) )
        {
            $value = $this->db->getUno( 0, $key );
            if( $value !== false )
                $value = $this->vi ? (int)$value[1] : $value[1];
        }
        else
            $value = false;        

        $this->kv[$key] = $value;
        if( isset( $this->vk ) )
            $this->vk[$value] = $key;
        $this->hits[$key] = 1;

        return $value;
    }

    public function getForcedKeyByValue( $value )
    {
        $key = $this->getKeyByValue( $value );
        if( $key !== false )
            return $key;

        if( !isset( $this->high ) )
            $this->setHigh();

        return $this->setKeyValue( ++$this->high, $value );
    }

    public function cacheHalving()
    {
        $this->merge();

        $kv = [];
        if( isset( $this->vk ) )
            $vk = [];
        $hits = [];

        arsort( $this->hits );
        $invalid = count( $this->hits ) >> 1;
        $i = 0;
        foreach( $this->hits as $key => $num )
        {
            if( ++$i > $invalid )
                break;
            
            $value = $this->kv[$key];
            $kv[$key] = $value;
            if( isset( $vk ) )
                $vk[$value] = $key;
            $hits[$key] = $num >> 1;
        }

        $this->kv = $kv;
        if( isset( $this->vk ) )
            $this->vk = $vk;
        $this->hits = $hits;
    }
}


/*
$db = '1.sqlite3';
$kv = new KV;
$kv->setStorage( $db, 'test', 0 );

$tt = microtime( true );

for( $i = 0; $i < 10000; ++$i )
{
    $value = $kv->getValueByKey( $i );
    $kv->setKeyValue( $i, "$i" );
}

echo sprintf( '%.02f', microtime( true ) - $tt ) . PHP_EOL;
$tt = microtime( true );

for( $i = 0; $i < 10000; ++$i )
{
    $value = $kv->getValueByKey( $i );
    //$key = $kv->getKeyByValue( "$i" );
}

$value = $kv->getValueByKey( 1337 );
$value = $kv->getValueByKey( 1337 );
$key = $kv->getKeyByValue( "1337" );
$key = $kv->getKeyByValue( "1338" );


echo sprintf( '%.02f', microtime( true ) - $tt ) . PHP_EOL;
$kv->cacheHalving();
$tt = microtime( true );

for( $i = 0; $i < 10000; ++$i )
{
    $value = $kv->getValueByKey( $i );
    //$key = $kv->getKeyByValue( "$i" );
}

echo sprintf( '%.02f', microtime( true ) - $tt );
/*

*/