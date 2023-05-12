<?php

namespace w8io;

use deemru\ABCode;

const UID = 0;
const TXKEY = 1;
const TYPE = 2;
const A = 3;
const B = 4;
const ASSET = 5;
const AMOUNT = 6;
const FEEASSET = 7;
const FEE = 8;
const ADDON = 9;
const GROUP = 10;

const GENESIS = 0;
const GENERATOR = -1;
const MATCHER = -2;
const MYSELF = -3;
const SPONSOR = -4;
const MASS = -5;

const TX_GENESIS = 1;
const TX_PAYMENT = 2;
const TX_ISSUE = 3;
const TX_TRANSFER = 4;
const TX_REISSUE = 5;
const TX_BURN = 6;
const TX_EXCHANGE = 7;
const TX_LEASE = 8;
const TX_LEASE_CANCEL = 9;
const TX_ALIAS = 10;
const TX_MASS_TRANSFER = 11;
const TX_DATA = 12;
const TX_SMART_ACCOUNT = 13;
const TX_SPONSORSHIP = 14;
const TX_SMART_ASSET = 15;
const TX_INVOKE = 16;
const TX_UPDATE_ASSET_INFO = 17;
const TX_ETHEREUM = 18;
const TX_EXPRESSION = 19;

const TX_GENERATOR = 0;
const TX_MATCHER = -1;
const TX_SPONSOR = -2;
const TX_REWARD = -7;

const ITX_ISSUE = -TX_ISSUE;
const ITX_TRANSFER = -TX_TRANSFER;
const ITX_REISSUE = -TX_REISSUE;
const ITX_BURN = -TX_BURN;
const ITX_LEASE = -TX_LEASE;
const ITX_LEASE_CANCEL = -TX_LEASE_CANCEL;
const ITX_SPONSORSHIP = -TX_SPONSORSHIP;
const ITX_INVOKE = -TX_INVOKE;

const TYPE_STRINGS =
[
    TX_SPONSOR => 'sponsor',
    TX_MATCHER => 'matcher',
    TX_GENERATOR => 'fees',
    TX_REWARD => 'reward',

    TX_GENESIS => 'genesis',
    TX_PAYMENT => 'payment',
    TX_ISSUE => 'issue',
    ITX_ISSUE => 'issue',
    TX_TRANSFER => 'transfer',
    ITX_TRANSFER => 'transfer',
    TX_REISSUE => 'reissue',
    ITX_REISSUE => 'reissue',
    TX_BURN => 'burn',
    ITX_BURN => 'burn',
    TX_EXCHANGE => 'exchange',
    TX_LEASE => 'lease',
    ITX_LEASE => 'lease',
    TX_LEASE_CANCEL => 'unlease',
    ITX_LEASE_CANCEL => 'unlease',
    TX_ALIAS => 'alias',
    TX_MASS_TRANSFER => 'mass',
    TX_DATA => 'data',
    TX_SMART_ACCOUNT => 'smart account',
    TX_SPONSORSHIP => 'sponsorship',
    ITX_SPONSORSHIP => 'sponsorship',
    TX_SMART_ASSET => 'smart asset',
    TX_INVOKE => 'invoke',
    ITX_INVOKE => 'invoke',
    TX_UPDATE_ASSET_INFO => 'rename',
    TX_ETHEREUM => 'ethereum',
    TX_EXPRESSION => 'expression',
];

const WAVES_ASSET = 0;
const NO_ASSET = -1;
const WAVES_LEASE_ASSET = -2;
const SPONSOR_ASSET = -3;

const FAILED_GROUP = -1;
const ETHEREUM_TRANSFER_GROUP = -2;

const EXPRESSION_FUNCTION = -1;

function w8k2i( $key ){ return $key & 0xFFFFFFFF; }
function w8k2h( $key ){ return $key >> 32; }
function w8h2k( $height, $i = 0 ){ return ( $height << 32 ) | $i; }
function w8h2kg( $height ){ return w8h2k( $height + 1 ) - 1; }
function d58( $data ){ return ABCode::base58()->decode( $data ); }
function e58( $data ){ return ABCode::base58()->encode( $data ); }
function json_unpack( $data ){ return json_decode( gzinflate( $data ), true, 512, JSON_BIGINT_AS_STRING ); }
function json_pack( $data ){ return gzdeflate( json_encode( $data ), 9 ); }

const TX_ASSET_IN = -10000;
const TX_ASSET_OUT = -20000;
function asset_in( $type ){ return TX_ASSET_IN - $type; }
function asset_out( $type ){ return TX_ASSET_OUT - $type; }

function w8io_amount( $amount, $decimals, $pad = 20, $setSign = true )
{
    if( $amount < 0 )
    {
        $sign = $setSign ? '-' : '';
        $amount = (string)-$amount;
    }
    else
    {
        $sign = '';
        $amount = (string)$amount;
    }

    if( $decimals )
    {
        if( strlen( $amount ) <= $decimals )
            $amount = str_pad( $amount, $decimals + 1, '0', STR_PAD_LEFT );
        $amount = substr_replace( $amount, '.', -$decimals, 0 );
    }

    $amount = $sign . $amount;
    return $pad ? str_pad( $amount, $pad, ' ', STR_PAD_LEFT ) : $amount;
}

function isAliasType( $type )
{
    switch( $type )
    {
        case TX_ALIAS:
        case TX_MASS_TRANSFER:

        case TX_TRANSFER:
        case TX_LEASE:
        case TX_INVOKE:

        case ITX_TRANSFER:
        case ITX_LEASE:
        case ITX_INVOKE:
            return true;

        default:
            return false;
    }
}

function w8enc( $data )
{
    return str_replace( [ 'i', '+', '/', '=' ], [ 'io', 'ip', 'is', ''], base64_encode( $data ) );
}

function w8dec( $data )
{
    return base64_decode( str_replace( 'io', 'i', str_replace( [ 'ip', 'is' ], [ '+', '/' ], $data ) ) );
}
