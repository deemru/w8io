<?php

namespace w8io;

use deemru\ABCode;

define( 'UID', 0 );
define( 'TXKEY', 1 );
define( 'TYPE', 2 );
define( 'A', 3 );
define( 'B', 4 );
define( 'ASSET', 5 );
define( 'AMOUNT', 6 );
define( 'FEEASSET', 7 );
define( 'FEE', 8 );
define( 'ADDON', 9 );
define( 'GROUP', 10 );

define( 'GENESIS', 0 );
define( 'GENERATOR', -1 );
define( 'MATCHER', -2 );
define( 'UNDEFINED', -3 );
define( 'SPONSOR', -4 );
define( 'MASS', -5 );

define( 'TX_SPONSOR', -2 );
define( 'TX_MATCHER', -1 );

define( 'TX_GENERATOR', 0 );
define( 'TX_GENESIS', 1 );
define( 'TX_PAYMENT', 2 );
define( 'TX_ISSUE', 3 );
define( 'TX_TRANSFER', 4 );
define( 'TX_REISSUE', 5 );
define( 'TX_BURN', 6 );
define( 'TX_EXCHANGE', 7 );
define( 'TX_LEASE', 8 );
define( 'TX_LEASE_CANCEL', 9 );
define( 'TX_ALIAS', 10 );
define( 'TX_MASS_TRANSFER', 11 );
define( 'TX_DATA', 12 );
define( 'TX_SMART_ACCOUNT', 13 );
define( 'TX_SPONSORSHIP', 14 );
define( 'TX_SMART_ASSET', 15 );
define( 'TX_INVOKE', 16 );
define( 'TX_UPDATE_ASSET_INFO', 17 );

define( 'SPONSOR_ASSET', -3 );
define( 'WAVES_LEASE_ASSET', -2 );
define( 'INVOKE_ASSET', -1 );
define( 'WAVES_ASSET', 0 );

define( 'FAILED_GROUP', -1 );

function w8k2i( $key ){ return $key & 0xFFFFFFFF; }
function w8k2h( $key ){ return $key >> 32; }
function w8h2k( $height, $i = 0 ){ return ( $height << 32 ) | $i; }
function d58( $data ){ return ABCode::base58()->decode( $data ); }
function e58( $data ){ return ABCode::base58()->encode( $data ); }
function json_unpack( $data ){ return json_decode( gzinflate( $data ), true, 512, JSON_BIGINT_AS_STRING ); }
function json_pack( $data ){ return gzdeflate( json_encode( $data ), 9 ); }

function w8io_amount( $amount, $decimals, $pad = 20, $setSign = true )
{
    if( $amount < 0 )
    {
        $sign = $setSign ? '-' : '';
        $amount = -$amount;
    }
    else
        $sign = '';

    $amount = (string)$amount;
    if( $decimals )
    {
        if( strlen( $amount ) <= $decimals )
            $amount = str_pad( $amount, $decimals + 1, '0', STR_PAD_LEFT );
        $amount = substr_replace( $amount, '.', -$decimals, 0 );
    }

    $amount = $sign . $amount;
    return $pad ? str_pad( $amount, $pad, ' ', STR_PAD_LEFT ) : $amount;
}

function w8io_tx_type( $type )
{
    switch( $type )
    {
        case TX_GENERATOR: return 'fees';
        case TX_SPONSOR: return 'sponsor';
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

function ptsFilter( $pts )
{
    $ptsInts = [];
    foreach( $pts as $ts )
        $ptsInts[] = [
            UID => (int)$ts[UID],
            TXKEY => (int)$ts[TXKEY],
            TYPE => (int)$ts[TYPE],
            A => (int)$ts[A],
            B => (int)$ts[B],
            ASSET => (int)$ts[ASSET],
            AMOUNT => (int)$ts[AMOUNT],
            FEEASSET => (int)$ts[FEEASSET],
            FEE => (int)$ts[FEE],
            ADDON => (int)$ts[ADDON],
            GROUP => (int)$ts[GROUP],
        ];
    return $ptsInts;
}
