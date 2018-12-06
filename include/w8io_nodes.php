<?php

require_once 'w8io_base.php';

class w8io_nodes
{
    private $cdb = [];
    private $cshift = 0;
    private $last_time = 0;
    private $ret_time;

    public function __construct( $hosts )
    {
        foreach( $hosts as $host )
            $this->cdb[] = [ 'host' => $host, 'curl' => false ];

        $this->ret_time = defined( 'W8IO_UPDATE_DELAY') ? max( W8IO_UPDATE_DELAY - 2, 1 ) : 1;
    }

    function __destruct()
    {
        $n = sizeof( $this->cdb );
        for( $i = 0; $i < $n; $i++ )
        {
            $c = &$this->cdb[$i];
            if( is_resource( $c['curl'] ) )
                curl_close( $c['curl'] );
        }
    }

    private function connect( $host )
    {
        if( false === ( $ch = curl_init() ) )
            w8io_error( 'curl_init() failed' );

        if( false === curl_setopt_array( $ch, [
            CURLOPT_CONNECTTIMEOUT  => 1,
            CURLOPT_TIMEOUT         => 15,
            CURLOPT_URL             => $host,
            CURLOPT_CONNECT_ONLY    => true,
            CURLOPT_CAINFO          => './third_party/ca-bundle/res/cacert.pem',
            //CURLOPT_SSL_VERIFYPEER  => false, // not secure
        ] ) )
            w8io_error( 'curl_setopt_array() failed' );

        $ms = 0;
        w8io_timer( $ms );
        {
            curl_exec( $ch );
        }
        $ms = w8io_ms( w8io_timer( $ms ) );

        if( 0 !== ( $errno = curl_errno( $ch ) ) )
        {
            w8io_trace( 'w', "$host error $errno: " . curl_error( $ch ) );
            curl_close( $ch );
            return false;
        }

        if( false === curl_setopt_array( $ch, [
            CURLOPT_RETURNTRANSFER  => true,
            CURLOPT_CONNECT_ONLY    => false,
        ] ) )
            w8io_error( 'curl_setopt_array() failed' );

        w8io_trace( 'i', "$host connected ($ms ms)" );
        return $ch;
    }

    private function connector()
    {
        $time = time();
        if( $this->cshift && $this->last_time && $time - $this->last_time > $this->ret_time )
        {
            w8io_trace( 'i', 'refresh connector' );
            $this->cshift = 0;
        }

        $n = sizeof( $this->cdb );
        for( ;; )
        {
            for( $i = $this->cshift; $i < $n; $i++ )
            {
                $c = &$this->cdb[$i];
                if( is_resource( $c['curl'] ) )
                {
                    $this->cshift = $i;
                    $this->last_time = $time;
                    return $c;
                }

                $ch = self::connect( $c['host'] );
                if( $ch )
                {
                    $c['curl'] = $ch;
                    $this->cshift = $i;
                    $this->last_time = $time;
                    return $c;
                }
            }

            w8io_trace( 'w', 'no connection...' );
            $this->cshift = 0;
            sleep( 1 );
        }
    }

    public function trynext()
    {
        $this->cshift = ( $this->cshift + 1 ) % sizeof( $this->cdb );
        $this->last_time = 0;
    }

    public function get( $url, $method = 'GET', $api = false, $data = false )
    {
        $c = $this->connector();
        $host = $c['host'];
        $ch = $c['curl'];
        $post = $method === 'POST';
        $options = [
            CURLOPT_HTTPHEADER      => $post ? [ 'Content-Type: application/json', 'Accept: application/json', $api ? "X-API-Key: $api" : '', ] : [],
            CURLOPT_URL             => $host . $url,
            CURLOPT_POST            => $post,
            CURLOPT_FOLLOWLOCATION  => true,
            CURLOPT_MAXREDIRS       => 3,
            // CURLOPT_SSL_VERIFYPEER  => false, // not secure
        ];

        if( $data )
            $options[CURLOPT_POSTFIELDS] = $data;

        if( false === curl_setopt_array( $ch, $options ) )
            w8io_error( 'curl_setopt_array() failed' );

        $ms = 0;
        w8io_timer( $ms );
        {
            $data = curl_exec( $ch );
            $code = curl_getinfo( $ch, CURLINFO_HTTP_CODE );
        }
        $ms = w8io_ms( w8io_timer( $ms ) );

        w8io_trace( 'i', "$host $method $url ($ms ms)" );

        if( 0 !== ( $errno = curl_errno( $ch ) ) || $code !== 200 )
        {
            if( $errno === 0 && $code === 404 )
                return false;

            $curl_error = curl_error( $ch );
            w8io_trace( 'w', "$host (HTTP $code) (cURL $errno" . ( empty( $curl_error ) ? ')' : ":$curl_error)" ) );
            curl_close( $ch );
            return false;
        }

        return $data;
    }

    public function broadcast( $tx )
    {
        $json = json_decode( self::get( '/transactions/broadcast', 'POST', false, json_encode( $tx ) ), true, 512, JSON_BIGINT_AS_STRING );

        if( !isset( $json['id'] ) )
            return false;

        return $json['id'];
    }

    public function txid( $id )
    {
        $json = json_decode( self::get( "/transactions/info/$id" ), true, 512, JSON_BIGINT_AS_STRING );

        if( !isset( $json['id'] ) || $json['id'] != $id )
            return false;

        return $json;
    }

    public function get_height()
    {
        $json = json_decode( self::get( '/blocks/height' ), true, 512, JSON_BIGINT_AS_STRING );

        if( !isset( $json['height'] ) )
            return false;

        return $json['height'];
    }

    public function get_block( $at )
    {
        $json = json_decode( self::get( "/blocks/at/$at" ), true, 512, JSON_BIGINT_AS_STRING );

        if( !isset( $json['generator'] ) )
            return false;

        return $json;
    }
}
