<?php

define( 'W8IO_NG_ACTIVE', W8IO_NETWORK == 'W' ? 805001 : 171001 );
define( 'W8IO_SPONSOR_ACTIVE', W8IO_NETWORK == 'W' ? 1080000 + 10000 : 339000 + 3000 );
define( 'W8IO_RESET_LEASES', W8IO_NETWORK == 'W' ? 462000 : 51500 );
define( 'W8IO_ASSET_EMPTY', -1 ); // to skip select with empty fee
define( 'W8IO_ASSET_WAVES_LEASED', -2 ); // to monitor waves leased
define( 'W8IO_TYPE_FEES', 0 ); // internal tx type for fees
define( 'W8IO_TYPE_SPONSOR', -1 ); // internal tx type for sponsor
define( 'W8IO_TYPE_INVOKE_DATA', -2 ); // internal tx type for invoke data
define( 'W8IO_TYPE_INVOKE_TRANSFER', -3 ); // internal tx type for invoke transfer

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

function w8io_tx_type( $type )
{
    switch( $type )
    {
        case W8IO_TYPE_INVOKE_TRANSFER: return 'invoke transfer';
        case W8IO_TYPE_INVOKE_DATA: return 'invoke data';
        case W8IO_TYPE_FEES: return 'fees';
        case W8IO_TYPE_SPONSOR: return 'sponsor';
        case 1: return 'genesis';
        case 101: return 'genesis role';
        case 102: return 'role';
        case 110: return 'genesis unknown';
        case 105: return 'data unknown';
        case 106: return 'invoke 1 unknown';
        case 107: return 'invoke 2 unknown';
        case 2: return 'payment';
        case 3: return 'issue';
        case 4: return 'transfer';
        case 5: return 'reissue';
        case 6: return 'burn';
        case 7: return 'exchange';
        case 8: return 'lease';
        case 9: return 'unlease';
        case 10: return 'alias';
        case 11: return 'mass';
        case 12: return 'data';
        case 13: return 'smart account';
        case 14: return 'sponsorship';
        case 15: return 'smart asset';
        case 16: return 'invoke';
        default: return 'unknown';
    }
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
