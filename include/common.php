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