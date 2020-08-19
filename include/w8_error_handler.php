<?php

function w8_bt( $errno, $errstr )
{
    $dbg = debug_backtrace();
    switch( $errno )
    {
        case E_ERROR: $level = 'E_ERROR'; break;
        case E_WARNING: $level = 'E_WARNING'; break;
        case E_PARSE: $level = 'E_PARSE'; break;
        case E_NOTICE: $level = 'E_NOTICE'; break;
        case E_CORE_ERROR: $level = 'E_CORE_ERROR'; break;
        case E_CORE_WARNING: $level = 'E_CORE_WARNING'; break;
        case E_COMPILE_ERROR: $level = 'E_COMPILE_ERROR'; break;
        case E_COMPILE_WARNING: $level = 'E_COMPILE_WARNING'; break;
        case E_USER_ERROR: $level = 'E_USER_ERROR'; break;
        case E_USER_WARNING: $level = 'E_USER_WARNING'; break;
        case E_USER_NOTICE: $level = 'E_USER_NOTICE'; break;
        case E_STRICT: $level = 'E_STRICT'; break;
        case E_RECOVERABLE_ERROR: $level = 'E_RECOVERABLE_ERROR'; break;
        case E_DEPRECATED: $level = 'E_DEPRECATED'; break;
        case E_USER_DEPRECATED: $level = 'E_USER_DEPRECATED'; break;
        case E_ALL: $level = 'E_ALL'; break;
        default: $level = 'E_UNKNOWN (' . $errno . ')'; break;
    }

    $bypass = [ __FUNCTION__, 'trigger_error' ];
    foreach( $dbg as $r )
        if( $r['function'] === 'trigger_error' )
        {
            $bypass[] = 'w8_error_handler';
            break;
        }

    $bt = $level . ': ' . $errstr;

    $n = count( $dbg );
    for( $i = 0; $i < $n; ++$i )
    {
        if( in_array( $dbg[$i]['function'], $bypass ) )
            continue;

        $bt .= PHP_EOL;
        $bt .= $function = empty( $dbg[$i + 1]['function'] ) ? 'root' : ( empty( $dbg[$i + 1]['class'] ) ? '' : $dbg[$i + 1]['class'] . '::' ) . $dbg[$i + 1]['function'] . '()';
        if( !isset( $f ) )
            $f = $function;
        if( isset( $dbg[$i]['file'] ) )
            $bt .= ' @ ' . ( $dbg[$i]['file'] ) . ':' . $dbg[$i]['line'];
    }

    return [ $f, $bt ];
}

function w8_error_handler( $errno, $errstr, $errfile, $errline )
{
    $bt = w8_bt( $errno, $errstr, $errfile, $errline );
    echo( PHP_EOL . $bt[1] . PHP_EOL );
    if( function_exists( 'w8_report' ) )
        w8_report( $bt[0], '<pre>' . $bt[1] . '</pre>' );
    exit( $errno );
}

set_error_handler( 'w8_error_handler' );
