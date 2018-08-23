<?php

namespace w8io_25519;

function gf( $init = false )
{
    $r = array_fill( 0, 16, 0 );

    if( $init !== false )
    {
        $n = count( $init );
        for( $i = 0; $i < $n; $i++ )
            $r[$i] = $init[$i];
    }

    return $r;
}

function car25519( &$o )
{
    $c = 1;

    for( $i = 0; $i < 16; $i++ )
    {
        $v = $o[$i] + $c + 65535;
        $c = floor( $v / 65536 );
        $o[$i] = $v - $c * 65536;
    }

    $o[0] += $c - 1 + 37 * ( $c - 1 );
}

function sel25519( &$p, &$q, $b )
{
    $c = ~( $b - 1 );

    for( $i = 0; $i < 16; $i++ )
    {
        $t = $c & ( $p[$i] ^ $q[$i] );
        $p[$i] ^= $t;
        $q[$i] ^= $t;
    }
}

function pack25519( &$o, $n )
{
    $m = gf();
    $t = gf();

    for( $i = 0; $i < 16; $i++ )
        $t[$i] = $n[$i];

    car25519( $t );
    car25519( $t );
    car25519( $t );

    for( $j = 0; $j < 2; $j++ )
    {
        $m[0] = $t[0] - 0xFFED;

        for( $i = 1; $i < 15; $i++ )
        {
          $m[$i] = $t[$i] - 0xFFFF - (( $m[$i - 1] >> 16 ) & 1 );
          $m[$i - 1] &= 0xFFFF;
        }

        $m[15] = $t[15] - 0x7FFF - (( $m[14] >> 16 ) & 1 );
        $b = ( $m[15] >> 16 ) & 1;
        $m[14] &= 0xFFFF;
        sel25519( $t, $m, 1 - $b );
    }

    for( $i = 0; $i < 16; $i++ )
    {
        $o[2 * $i] = $t[$i] & 0xFF;
        $o[2 * $i + 1] = $t[$i] >> 8;
    }
}

function par25519( $a )
{
    $d = array_fill( 0, 32, 0 );
    pack25519( $d, $a );
    return $d[0] & 1;
}

function unpack25519( &$o, $n )
{
    for( $i = 0; $i < 16; $i++ )
        $o[$i] = $n[2 * $i] + ( $n[2 * $i + 1] << 8 );

    $o[15] &= 0x7FFF;
}

function A( &$o, $a, $b )
{
    for( $i = 0; $i < 16; $i++ )
        $o[$i] = $a[$i] + $b[$i];
}

function Z( &$o, $a, $b )
{
    for( $i = 0; $i < 16; $i++ )
        $o[$i] = $a[$i] - $b[$i];
}

function M( &$o, $a, $b )
{
    $t = array_fill( 0, 31, 0 );

    for( $i = 0; $i < 16; $i++ )
        for( $j = 0; $j < 16; $j++ )
            $t[$i + $j] += $a[$i] * $b[$j];

    for( $i = 0; $i < 15; $i++ )
        $t[$i] += 38 * $t[$i + 16];

    for( $i = 0; $i < 16; $i++ )
        $o[$i] = $t[$i];

    car25519( $o );
    car25519( $o );
}

function S( &$o, $a )
{
    M( $o, $a, $a );
}

function inv25519( &$o, $i )
{
    $c = gf();

    for( $a = 0; $a < 16; $a++ )
        $c[$a] = $i[$a];

    for( $a = 253; $a >= 0; $a-- )
    {
        S( $c, $c );
        if( $a !== 2 && $a !== 4 )
            M( $c, $c, $i );
    }

    for( $a = 0; $a < 16; $a++ )
        $o[$a] = $c[$a];
}

function add( &$p, $q )
{
    $a = gf(); $b = gf(); $c = gf();
    $d = gf(); $e = gf(); $f = gf();
    $g = gf(); $h = gf(); $t = gf();
    $D2 = gf( [ 0xF159, 0x26B2, 0x9B94, 0xEBD6, 0xB156, 0x8283, 0x149A, 0x00E0, 0xD130, 0xEEF3, 0x80F2, 0x198E, 0xFCE7, 0x56DF, 0xD9DC, 0x2406 ] );

    Z( $a, $p[1], $p[0] );
    Z( $t, $q[1], $q[0] );
    M( $a, $a, $t );
    A( $b, $p[0], $p[1] );
    A( $t, $q[0], $q[1] );
    M( $b, $b, $t );
    M( $c, $p[3], $q[3] );
    M( $c, $c, $D2 );
    M( $d, $p[2], $q[2] );
    A( $d, $d, $d );
    Z( $e, $b, $a );
    Z( $f, $d, $c );
    A( $g, $d, $c );
    A( $h, $b, $a );

    M( $p[0], $e, $f );
    M( $p[1], $h, $g );
    M( $p[2], $g, $f );
    M( $p[3], $e, $h );
}

function cswap( &$p, &$q, $b )
{
    for( $i = 0; $i < 4; $i++ )
        sel25519( $p[$i], $q[$i], $b );
}

function pack( &$r, $p )
{
    $tx = gf();
    $ty = gf();
    $zi = gf();

    inv25519( $zi, $p[2] );
    M( $tx, $p[0], $zi );
    M( $ty, $p[1], $zi );
    pack25519( $r, $ty );
    $r[31] ^= par25519( $tx ) << 7;
}

function scalarmult( &$p, &$q, $s )
{
    $p[0] = gf();
    $p[1] = gf( [ 1 ] );
    $p[2] = gf( [ 1 ] );
    $p[3] = gf();

    for( $i = 255; $i >= 0; $i-- )
    {
        $b = ( $s[( $i / 8 ) | 0] >> ( $i & 7 )) & 1;
        cswap( $p, $q, $b );
        add( $q, $p );
        add( $p, $p );
        cswap( $p, $q, $b );
    }
}

function scalarbase( &$p, $s )
{
    $X = gf( [ 0xD51A, 0x8F25, 0x2D60, 0xC956, 0xA7B2, 0x9525, 0xC760, 0x692C, 0xDC5C, 0xFDD6, 0xE231, 0xC0A4, 0x53FE, 0xCD6E, 0x36D3, 0x2169 ] );
    $Y = gf( [ 0x6658, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666, 0x6666 ] );

    $q = array();
    $q[0] = $X;
    $q[1] = $Y;
    $q[2] = gf( [ 1 ] );
    $q[3] = array();
    M( $q[3], $X, $Y );

    scalarmult( $p, $q, $s );
}

function modL( &$r, &$x )
{
    $L = array_merge( [ 0xED, 0xD3, 0xF5, 0x5C, 0x1A, 0x63, 0x12, 0x58, 0xD6, 0x9C, 0xF7, 0xA2, 0xDE, 0xF9, 0xDE, 0x14, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10 ], array_fill( 0, 32, 0 ) );

    for( $i = 63; $i >= 32; $i-- )
    {
        $carry = 0;

        for( $j = $i - 32, $k = $i - 12; $j < $k; $j++ )
        {
            $x[$j] += $carry - 16 * $x[$i] * $L[$j - ( $i - 32 )];
            $carry = ( $x[$j] + 128 ) >> 8;
            $x[$j] -= $carry * 256;
        }

        $x[$j] += $carry;
        $x[$i] = 0;
    }

    $carry = 0;

    for( $j = 0; $j < 32; $j++ )
    {
        $x[$j] += $carry - ( $x[31] >> 4 ) * $L[$j];
        $carry = $x[$j] >> 8;
        $x[$j] &= 255;
    }

    for( $j = 0; $j < 32; $j++ )
        $x[$j] -= $carry * $L[$j];

    for( $i = 0; $i < 32; $i++ )
    {
        $x[$i+1] += $x[$i] >> 8;
        $r[$i] = $x[$i] & 255;
    }
}

function reduce( &$r )
{
    $x = array_fill( 0, 64, 0 );

    for( $i = 0; $i < 64; $i++ )
        $x[$i] = $r[$i];

    for( $i = 0; $i < 64; $i++ )
        $r[$i] = 0;

    modL( $r, $x );
}

function sha512( $data, $len )
{
    $raw = '';
    for( $i = 0; $i < $len; $i++ )
        $raw .= chr( $data[$i] );

    $raw = hash( 'sha512', $raw, true );

    $hash = array();
    for( $i = 0; $i < 64; $i++ )
        $hash[] = ord( $raw[$i] );

    return $hash;
}

function sign_direct( $msg, $sk, $rnd )
{
    $n = strlen( $msg );
    $sm = array_fill( 0, $n + 128, 0 );

    $sm[0] = 0xFE;
    for( $i = 1; $i < 32; $i++ )
        $sm[$i] = 0xFF;

    for( $i = 0; $i < 32; $i++ )
        $sm[32 + $i] = $sk[$i];

    for( $i = 0; $i < $n; $i++ )
        $sm[64 + $i] = ord( $msg[$i] );

    for( $i = 0; $i < 64; $i++ )
        $sm[$n + 64 + $i] = ord( $rnd[$i] );

    $r = sha512( $sm, $n + 128 );
    reduce( $r );
    $p = [ gf(), gf(), gf(), gf() ];
    scalarbase( $p, $r );
    pack( $sm, $p );

    for( $i = 0; $i < 32; $i++ )
        $sm[$i + 32] = $sk[32 + $i];

    $h = sha512( $sm, $n + 64 );
    reduce( $h );

    for( $i = 0; $i < 64; $i++ )
        $sm[$n + 64 + $i] = 0;

    $x = array();
    for( $i = 0; $i < 32; $i++ )
        $x[] = $r[$i];

    for( $i = 32; $i < 64; $i++ )
        $x[] = 0;

    for( $i = 0; $i < 32; $i++ )
        for( $j = 0; $j < 32; $j++ )
            $x[$i + $j] += $h[$i] * $sk[$j];

    $sm32 = array_fill( 0, 32, 0 );
    modL( $sm32, $x );

    return array_merge( array_slice( $sm, 0, 32 ), $sm32 );
}

function curve25519_to_ed25519( $pk )
{
    $z = array_fill( 0, 32, 0 );
    $x = gf(); $a = gf(); $b = gf();
    $gf1 = gf( [ 1 ] );

    unpack25519( $x, $pk );

    A( $a, $x, $gf1 );
    Z( $b, $x, $gf1 );
    inv25519( $a, $a );
    M( $a, $a, $b );

    pack25519( $z, $a );
    return $z;
}

function curve25519_sign( $msg, $key )
{
    if( strlen( $key ) !== 32 )
        return false;

    $edsk = array();
    for( $i = 0; $i < 32; $i++ )
        $edsk[] = ord( $key[$i] );

    $edsk[0] &= 248;
    $edsk[31] &= 127;
    $edsk[31] |= 64;

    $p = [ gf(), gf(), gf(), gf() ];
    scalarbase( $p, $edsk );
    $edsk32 = array_fill( 0, 32, 0 );
    pack( $edsk32, $p );
    $edsk = array_merge( $edsk, $edsk32 );

    $signBit = $edsk[63] & 128;
    $sm = sign_direct( $msg, $edsk, random_bytes( 64 ) );
    $sm[63] |= $signBit;

    $signature = '';
    for( $i = 0; $i < 64; $i++ )
        $signature .= chr( $sm[$i] );

    return $signature;
};

function curve25519_verify( $signature, $msg, $key )
{
    if( strlen( $key ) !== 32 || strlen( $signature ) !== 64 )
        return false;

    $pk = array();
    for( $i = 0; $i < 32; $i++ )
        $pk[] = ord( $key[$i] );

    $edpk = curve25519_to_ed25519( $pk );
    $edpk[31] |= ord( $signature[63] ) & 128;
    $signature[63] = chr( ord( $signature[63] ) & 127 );

    $key = '';
    for( $i = 0; $i < 32; $i++ )
        $key .= chr( $edpk[$i] );

    return sodium_crypto_sign_verify_detached( $signature, $msg, $key );
};
