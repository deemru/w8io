<?php

function w8io_error_handler( $errno, $errstr, $errfile )
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

    $err = str_pad( "$err: $errstr ($errfile)", 64, '-' );
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
