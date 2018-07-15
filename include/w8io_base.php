<?php

define( 'W8IO_NG_ACTIVE', W8IO_NETWORK == 'W' ? 805001 : 171001 );
define( 'W8IO_SPONSOR_ACTIVE', W8IO_NETWORK == 'W' ? 1090000 : 369000 );
define( 'W8IO_ASSET_EMPTY', -1 ); // to skip select with empty fee
define( 'W8IO_ASSET_WAVES_LEASED', -2 ); // to monitor waves leased
define( 'W8IO_TYPE_FEES', 0 ); // internal tx type for fees
define( 'W8IO_TYPE_SPONSOR', -1 ); // internal tx type for sponsor

function w8io_err( $errno , $errstr, $errfile, $errline )
{
    switch( $errno )
    {
        case E_ERROR: $err = 'E_ERROR'; break;
        case E_WARNING: $err = 'E_WARNING'; break;
        case E_PARSE: $err = 'E_PARSE'; break;
        case E_NOTICE: $err = 'E_NOTICE'; break;
        case E_CORE_ERROR: $err = 'E_CORE_ERROR'; break;
        case E_CORE_WARNING: $err = 'E_CORE_WARNING'; break;
        case E_COMPILE_ERROR: $err = 'E_COMPILE_ERROR'; break;
        case E_COMPILE_WARNING: $err = 'E_COMPILE_WARNING'; break;
        case E_USER_ERROR: $err = 'E_USER_ERROR'; break;
        case E_USER_WARNING: $err = 'E_USER_WARNING'; break;
        case E_USER_NOTICE: $err = 'E_USER_NOTICE'; break;
        case E_STRICT: $err = 'E_STRICT'; break;
        case E_RECOVERABLE_ERROR: $err = 'E_RECOVERABLE_ERROR'; break;
        case E_DEPRECATED: $err = 'E_DEPRECATED'; break;
        case E_USER_DEPRECATED: $err = 'E_USER_DEPRECATED'; break;
        case E_ALL: $err = 'E_ALL'; break;
        default: $err = 'E_UNKNOWN'; break;
    }

    $err = str_pad( "$err: $errstr ", 64, '-' );
    $dbg = debug_backtrace();

    for( $i = 1, $n = sizeof( $dbg ); $i < $n; $i++ )
    {
        $err .= PHP_EOL;
        $err .= str_pad( empty( $dbg[$i]['function'] ) ? 'root()' : ( empty( $dbg[$i]['class'] ) ? '' : "{$dbg[$i]['class']}::" ) . "{$dbg[$i]['function']}()", 32 );
        $err .= str_pad( substr( $dbg[$i]['file'], strrpos( $dbg[$i]['file'], '\\' ) + 1 ) . ":{$dbg[$i]['line']}", 32 );
    }
    $err .= PHP_EOL . str_pad( '', 64, '-' );

    echo( PHP_EOL . $err . PHP_EOL );
    exit;
}

set_error_handler( 'w8io_err' );

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

function w8io_trace( $level, $message )
{
    echo w8io_log( $level, $message );
}

function w8io_warning( $message )
{
    w8io_trace( 'w', $message );
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
        case W8IO_TYPE_FEES: return 'fees';
        case W8IO_TYPE_SPONSOR: return 'sponsor';
        case 1: return 'genesis';
        case 2: return 'payment';
        case 3: return 'issue';
        case 4: return 'transfer';
        case 5: return 'reissue';
        case 6: return 'burn';
        case 7: return 'exchange';
        case 8: return 'lease';
        case 9: return 'cancel lease';
        case 10: return 'alias';
        case 11: return 'mass transfer';
        case 12: return 'data';
        case 13: return 'script';
        case 14: return 'sponsorship';
        default: return 'unkonwn';
    }
}
