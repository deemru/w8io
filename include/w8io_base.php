<?php

use deemru\ABCode;
function base58Encode( $data ){ return ABCode::base58()->encode( $data ); }
function base58Decode( $data ){ return ABCode::base58()->decode( $data ); }

function w8io_timer( &$timer )
{
    $now = microtime( true );
    $elapsed = $now - $timer;
    $timer = $now;
    return $elapsed * 1000;
}

function w8io_ms( $ms )
{
    if( $ms > 100 )
        return round( $ms );
    else if( $ms > 10 )
        return sprintf( '%.01f', $ms );
    return sprintf( '%.02f', $ms );
}

function w8io_log( $level, $message )
{
    $log = date( 'Y.m.d H:i:s ' );
    switch( $level )
    {
        case 'd': $log .= '  DEBUG: '; break;
        case 'w': $log .= 'WARNING: '; break;
        case 'e': $log .= '  ERROR: '; break;
        case 'i': $log .= '   INFO: '; break;
        case 's': $log .= 'SUCCESS: '; break;
        default:  $log .= 'UNKNOWN: '; break;
    }
    return $log . $message . PHP_EOL;
}

function w8io_trace( $level, $message, $ex = null )
{
    static $exclude = '';

    if( isset( $ex ) )
        $exclude = $ex;

    if( false !== strpos( $exclude, $level ) )
        return;

    echo w8io_log( $level, $message );
}

function w8io_warning( $message )
{
    w8io_trace( 'w', $message );
}

function w8io_info( $message )
{
    w8io_trace( 'i', $message );
}

function w8io_error( $message = false )
{
    trigger_error( $message ? w8io_log( 'e', $message ) : '(no message)', E_USER_ERROR );
}

function mb_str_pad( $input, $pad_length, $pad_string, $pad_style )
{ 
    return str_pad( $input, strlen( $input ) - mb_strlen( $input,'UTF-8' ) + $pad_length, $pad_string, $pad_style );
}

function w8io_aggregate_jsons( $db, $from, $to, $q, $sum = [] )
{       
    for( $i = $from; $i <= $to; $i += $q )
    {
        $json = $db->getValue( $i, 'j' );

        if( false === $json )
            w8io_error( 'getValue() failed' );

        foreach( $json as $type => $value )
        {
            if( isset( $sum[$type] ) )
                $sum[$type] += $value;
            else
                $sum[$type] = $value;
        }
    }

    return $sum;
}

function w8io_filter_wtx( $wtx )
{
    return 
    [
        'uid' => isset( $wtx['uid'] ) ? (int)$wtx['uid'] : null,
        'txid' => (int)$wtx['txid'],
        'block' => (int)$wtx['block'],
        'type' => (int)$wtx['type'],
        'timestamp' => (int)$wtx['timestamp'],
        'a' => (int)$wtx['a'],
        'b' => (int)$wtx['b'],
        'amount' => (int)$wtx['amount'],
        'asset' => (int)$wtx['asset'],
        'fee' => (int)$wtx['fee'],
        'afee' => (int)$wtx['afee'],
        'data' => empty( $wtx['data'] ) ? false : $wtx['data'],
    ];
}
