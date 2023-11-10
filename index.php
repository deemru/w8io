<?php

namespace w8io;
require_once 'config.php';

$z = (int)( $_COOKIE['z'] ?? 180 ); // TIMEZONE

if( isset( $_SERVER['REQUEST_URI'] ) )
    $urio = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $urio = '3PNikM6yp4NqcSU8guxQtmR5onr2D4e8yTJ/data';

$uri = preg_filter( '/[^a-zA-Z0-9_.@\-\/]+/', '', $urio . chr( 0 ) );
if( $uri === '' )
    $uri = [ 'GENERATORS' ];
else
{
    if( $uri[strlen($uri) - 1] === '/' )
        $uri = substr( $uri, 0, -1 );
    $uri = explode( '/', $uri );
}

$address = $uri[0];
if( isset( $uri[1] ) )
{
    $f = $uri[1];
    if( isset( $uri[2] ) )
    {
        $arg = $uri[2];
        if( isset( $uri[3] ) )
        {
            $arg2 = $uri[3];
            if( isset( $uri[4] ) )
                $arg3 = $uri[4];
            else
                $arg3 = false;
        }
        else
        {
            $arg2 = false;
            $arg3 = false;
        }
    }
    else
    {
        $arg = false;
        $arg2 = false;
        $arg3 = false;
    }
}
else
{
    $f = false;
    $arg = false;
    $arg2 = false;
    $arg3 = false;
}

if( $address === 'api' )
{
    require_once 'include/RO.php';

    function apiexit( $code, $json )
    {
        http_response_code( $code );
        exit( json_encode( $json ) );
    }

    if( strlen( $f ) > 20 )
    {
        $wk = wk();
        if( false === ( $f = w8dec( $f ) ) ||
            false === ( $call = $wk->decryptash( $f ) ) ||
            false === ( $call = $wk->json_decode( $call ) ) )
            exit( $wk->log( 'e', 'bad API call' ) );

        switch( $call['f'] )
        {
            case 't':
            {
                $RO = new RO( W8DB );

                $aid = $call['i'];
                $where = $call['w'];
                $uid = $call['u'];
                $address = $call['a'];
                $d = $call['d'] ?? 3;

                echo '<pre>';
                w8io_print_transactions( $aid, $where, $uid, 100, $address, $d );
                echo '</pre>';
                return;
            }
            case 'd':
            {
                $RO = new RO( W8DB );

                $address = $call['a'];
                $aid = $call['i'];
                $begin = $call['b'];
                $limit = $call['l'];

                echo '<pre>';
                [ $data, $lazy ] = w8io_get_data( $address, $aid, $begin, $limit );
                w8io_print_data( '<a href="' . W8IO_ROOT . $address . '/data/', $data, $lazy );
                echo '</pre>';
                return;
            }
        }

        exit( $wk->log( 'e', 'bad API call' ) );
    }
    else
    if( $f === 'height' )
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );
        $json = $RO->getLastHeightTimestamp();
        if( $json === false )
            apiexit( 503, [ 'code' => 503, 'message' => 'database unavailable' ] );
        apiexit( 200, $json[0] );
    }
    else
    if( $f === 'alive' )
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );
        $json = $RO->getLastHeightTimestamp();
        if( $json === false )
            apiexit( 503, [ 'code' => 503, 'message' => 'database unavailable' ] );
        $now = time();
        $dbts = $json[1];
        $diff = $now - $dbts;
        $threshold = $arg === false ? 600 : intval( $arg );
        if( $diff > $threshold )
            apiexit( 503, [ 'code' => 503, 'message' => "too big diff: $now - $dbts = $diff > $threshold" ] );
        apiexit( 200, $diff );
    }

    exit( http_response_code( 404 ) );
}

if( $address === 'j13' )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );
    $q = $RO->db->query( 'SELECT * FROM pts WHERE r2 = 13 ORDER BY r0 DESC LIMIT 100' );
    $n = 0;
    $json = [];
    $max = 100;
    if( $f !== false )
        $max = min( (int)$f, $max );
    foreach( $q as $r )
    {
        $txkey = $r[1];
        $txid = $RO->getTxIdByTxKey( $txkey );
        $json[] = $txid;
        if( ++$n >= $max )
            break;
    }

    exit( json_encode( $json ) );
}

if( $address === 'tx' && is_numeric( $f ) )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    if( w8k2h( $f ) === w8k2h( $f + 1 ) - 1 )
        exit( header( 'location: ' . W8IO_ROOT . 'b/' . w8k2h( $f ) ) );

    $txid = $RO->getTxIdByTxKey( $f );
    if( $txid !== false )
        exit( header( 'location: ' . W8IO_ROOT . 'tx/' . $txid ) );
}
else
if( $address === 'tk' && $f !== false && strlen( $f ) >= 32 )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    $txkey = $RO->getTxKeyByTxId( $f );
    exit( (string)$txkey );
}
else
if( $address === 'top' && $f !== false )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    if( is_numeric( $f ) )
    {
        $f = $RO->getAssetById( $f );
        if( $f !== false )
            exit( header( 'location: ' . W8IO_ROOT . 'top/' . $f ) );
    }

    $aid = $f === 'Waves' ? 0 : $RO->getIdByAsset( $f );
    if( $aid !== false )
    {
        $info = $RO->getAssetInfoById( $aid );
        if( $info !== false )
        {
            if( $arg === false )
                $arg = 1000;
            else if( $arg > 10000 )
                exit( header( 'location: ' . W8IO_ROOT . 'top/' . $f . '/10000' ) );
        }
        else
        {
            unset( $info );
            unset( $aid );
        }
    }
}
else
if( $address === 'GENERATORS' )
{
    //$showtime = true;

    if( $f === false )
        $f = 1472;

    $f = intval( $f );
    $n = min( max( $f, isset( $showtime ) ? 1 : 80 ), 100000 );
    if( $n !== $f )
        exit( header("location: " . W8IO_ROOT . "$address/$n" ) );

    if( isset( $showtime ) )
    {
        $showfile = "GENERATORS-$arg-$f.html";
        if( file_exists( $showfile ) )
            exit( file_get_contents( $showfile ) );

        ob_start();
    }
}

function prettyAddress( $address )
{
    if( strlen( $address ) === 35 )
        return substr( $address, 0, 6 ) . '&#183;&#183;&#183;' . substr( $address, -4 );
    return $address;
}

if( strlen( $address ) > 35 )
{
    $f = $address;
    $address = 'tx';
}

function prolog()
{
    global $address;
    global $L;

    $L = (int)( $_COOKIE['L'] ?? 0 ) === 1;
    echo sprintf( '
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="format-detection" content="telephone=no">
        <meta name="format-detection" content="date=no">
        <meta name="format-detection" content="address=no">
        <meta name="format-detection" content="email=no">
        <title>%s</title>
        <link rel="shortcut icon" href="/static/favicon8.ico" type="image/x-icon">
        <link rel="stylesheet" href="/static/fonts.css">
        <link rel="stylesheet" href="/static/static%s.css">
        <script type="text/javascript" src="/static/jquery.js" charset="UTF-8"></script>
        <script type="text/javascript" src="/static/static.js" charset="UTF-8"></script>
    </head>
    <body>
        <pre>
', empty( $address ) ? '' : ( 'w8 &#183; ' . prettyAddress( $address ) ), $L ? '-l' : '-n' );
}

function w8io_get_data( $address, $aid, $begin, $limit )
{
    global $RO;

    $rs = $RO->getKVsByAddress( $aid, $begin, $limit + 1 );
    $n = 0;
    foreach( $rs as [ $r0, /*$r1*/, /*$r2*/, /*$r3*/, $r4, $r5, $r6 ] )
    {
        if( ++$n > $limit )
        {
            $wk = wk();
            $call = [
                'f' => 'd',
                'a' => $address,
                'i' => $aid,
                'b' => $r0,
                'l' => $limit,
            ];
            $call = W8IO_ROOT . 'api/' . w8enc( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            break;
        }

        if( $r6 === TYPE_NULL )
            continue;

        $key = $RO->getKeyById( $r4 );
        $value = $RO->getValueByTypeId( $r6, $r5 );
        $data[] = [ 'key' => $key, 'type' => DATA_TYPE_STRINGS[$r6], 'value' => $value ];
    }

    return [ $data ?? [], $lazy ?? false ];
}

function w8io_print_data( $datauri, $data, $lazy )
{
    $n = 0;
    foreach( $data as $r )
    {
        $k = htmlentities( $r['key'] );
        $t = $r['type'];
        if( $t === 'string' )
            $v = '"' . htmlentities( $r['value'] ) . '"';
        else if( $t === 'binary' )
            $v = '"' . $r['value'] . '"';
        else if( $t === 'boolean' )
            $v = $r['value'] ? 'true' : 'false';
        else
            $v = $r['value'];
        if( ++$n > 1 )
            echo ',';
        echo PHP_EOL . '    "' . $datauri . urlencode( $k ) . '">' . $k . '</a>": ' . $v;
    }
    if( $lazy === false )
        echo PHP_EOL . '}';
    else
        echo PHP_EOL . $lazy;
}

function w8io_print_distribution( $f, $aid, $info, $n )
{
    global $z;
    global $RO;

    $decimals = ( $decimals = $info[0] ) === 'N' ? 0 : (int)$decimals;
    $asset = substr( $info, 2 );

    $balances = $RO->db->query( 'SELECT * FROM balances WHERE r2 = ' . $aid . ' ORDER BY r3 DESC LIMIT ' . $n );
    $total = 0;
    $n = 0;
    $out = '';
    foreach( $balances as $balance )
    {
        $amount = (int)$balance[3];
        if( $amount <= 0 )
            break;
        $total += $amount;
        $aid = (int)$balance[1];
        $address = $RO->getAddressById( $aid );
        $out .= str_pad( ++$n, 5, ' ', STR_PAD_LEFT ) . ') <a href="' . W8IO_ROOT . $address . '/f/' . $f . '">' . $address . '</a>: ' . w8io_amount( $amount, $decimals ) . PHP_EOL;
    }

    $heightTime = $RO->getLastHeightTimestamp();
    $time = date( 'Y.m.d H:i', $heightTime[1] + $z * 60 );
    $height = $heightTime[0];

    echo 'Top ' . $n . ' (' . $asset .') @ ' . $height . ' <small>(' . $time . ')</small>'. PHP_EOL . PHP_EOL;
    echo str_pad( 'Top ' . $n . ' balance: ', 44, ' ', STR_PAD_LEFT ) . w8io_amount( $total, $decimals ) . PHP_EOL . PHP_EOL;
    echo $out;
}

function w8io_sign( $sign )
{
    if( $sign > 0 )
        return '+';
    if( $sign < 0 )
        return '-';
    return '';
}

function w8io_print_transactions( $aid, $where, $uid, $count, $address, $d )
{
    global $RO;
    global $z;

    $pts = $RO->getPTSByAddressId( $aid, $where, $count + 1, $uid, $d );
    //$pts = $RO->getPTSAtHeight( 2214328 );

    //$wtxs = $api->get_transactions_where( $aid, $where, $uid, $count + 1 );

    $maxlen1 = 0;
    $maxlen2 = 0;
    $outs = [];
    $tdb = [];
    $lastblock = 0;

    $n = 0;
    foreach( $pts as $ts )
    {
        if( $count && ++$n > $count )
        {
            $wk = wk();
            $call = [
                'f' => 't',
                'i' => $aid,
                'w' => $where,
                'u' => $ts[UID],
                'a' => $address,
                'd' => $d,
            ];
            $call = W8IO_ROOT . 'api/' . w8enc( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            break;
        }

        $type = $ts[TYPE];
        $asset = $ts[ASSET];
        $amount = $ts[AMOUNT];
        $a = $ts[A];
        $b = $ts[B];
        $aspam = false;
        unset( $reclen );
        if( $type === TX_SPONSORSHIP )
        {
            if( $aid !== false && $a !== $aid )
                continue;
            $b = MYSELF;
        }
        if( $aid !== false && $b === MYSELF )
        {
            $b = $a;
            $isa = true;
            $isb = true;

            if( $asset === NO_ASSET )
            {
                $amount = '';
                $asset = '';
                $reclen = -1;
            }
        }
        else
        {
            $isa = $a === $aid;
            $isb = $b === $aid;
        }

        if( !isset( $reclen ) )
        {
            if( $asset > 0 )
            {
                if( $type === TX_ISSUE || $type === TX_REISSUE )
                    $sign = 1;
                else if( $type === TX_BURN )
                    $sign = -1;
                else
                    $sign = ( $amount < 0 ? -1 : 1 ) * ( $isb ? ( $isa ? 0 : 1 ) : -1 );

                $info = $RO->getAssetInfoById( $asset );
                if( $info[1] === chr( 1 ) )
                    $aspam = true;

                $name = substr( $info, 2 );
                $decimals = ( $decimals = $info[0] ) === 'N' ? 0 : (int)$decimals;
                $amount = w8io_amount( $amount, $decimals, 0, false );
                $amount = ' ' . w8io_sign( $sign ) . $amount;
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . $asset . '">' . $name . '</a>';
                $reclen = strlen( $amount ) + mb_strlen( html_entity_decode( $name ), 'UTF-8' );
            }
            else if( $amount === 0 && ( $type === TX_INVOKE || $type === ITX_INVOKE || $type === TX_ETHEREUM ) )
            {
                $amount = '';
                $asset = '';
                $reclen = -1;
            }
            else
            {
                $sign = ( ( $type === TX_LEASE_CANCEL || $type === ITX_LEASE_CANCEL ) ? -1 : 1 ) * ( ( $amount < 0 ) ? -1 : 1 ) * ( $isb ? ( $isa ? 0 : 1 ) : -1 );

                $amount = ' ' . w8io_sign( $sign ) . w8io_amount( $amount, 8, 0, false );
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/Waves">Waves</a>';
                $reclen = strlen( $amount ) + 5;
            }
        }

        if( $type == TX_ISSUE )
            $type = 3;

        $a = $isa ? $address : $RO->getAddressById( $a );
        $b = $isb ? $address : $RO->getAddressById( $b );

        $fee = $ts[FEE];

        if( $isa && $fee )
        {
            $afee = $ts[FEEASSET];

            if( $afee )
            {
                $info = $RO->getAssetInfoById( $afee );
                $decimals = ( $decimals = $info[0] ) === 'N' ? 0 : (int)$decimals;
                $fee = ' <small>' . w8io_amount( $fee, $decimals, 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/' . $afee . '">' . substr( $info, 2 ) . '</a></small>';
            }
            else
            {
                $fee = ' <small>' . w8io_amount( $fee, 8, 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/Waves">Waves</a></small>';
            }
        }
        else
            $fee = '';

        $addon = $ts[ADDON];
        $linklen = 0;

        if( $addon === 0 )
            $addon = '';
        else
        {
            if( isAliasType( $type ) )
            {
                $b = $RO->getAliasById( $addon );
                $addon = '';
                $isb = false;
            }
            else if( $type === TX_EXCHANGE )
            {
                $groupId = $ts[GROUP];

                if( isset( $tdb[$groupId] ) )
                {
                    [ $bdecimals, $bname, $sdecimals, $sname, $link, $linklen ] = $tdb[$groupId];
                }
                else
                {
                    $group = $RO->getGroupById( $groupId );
                    if( $group === false )
                        w8_err( "getGroupById( $groupId )" );
                    $pair = explode( ':', substr( $group, 1 ) );
                    $buy = $RO->getAssetInfoById( (int)$pair[0] );
                    $sell = $RO->getAssetInfoById( (int)$pair[1] );

                    $bdecimals = ( $bdecimals = $buy[0] ) === 'N' ? 0 : (int)$bdecimals;
                    $bname = substr( $buy, 2 );
                    $sdecimals = ( $sdecimals = $sell[0] ) === 'N' ? 0 : (int)$sdecimals;
                    $sname = substr( $sell, 2 );

                    $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                    $linklen = strlen( $link ) + 3;

                    $tdb[$groupId] = [ $bdecimals, $bname, $sdecimals, $sname, $link, $linklen ];
                }

                $price = w8io_amount( $addon, $sdecimals, 0 );

                $addon = ' ' . $price . $link . $bname . '/' . $sname . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );
            }
            else
                $addon = '';
        }

        if( $type === TX_INVOKE || $type === TX_ETHEREUM || $type <= ITX_ISSUE )
        {
            $groupId = $ts[GROUP];

            if( isset( $tdb[$groupId] ) )
            {
                [ $link, $linklen, $addon, $maxlen ] = $tdb[$groupId];
                if( $maxlen > $maxlen2 )
                    $maxlen2 = $maxlen;
            }
            if( $groupId > 0 )
            {
                $group = $RO->getGroupById( $groupId );
                if( $group === false )
                    w8_err( "getGroupById( $groupId )" );
                $addon = $RO->getFunctionById( (int)explode( ':', $group )[1] );

                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . $addon . '()</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );

                $tdb[$groupId] = [ $link, $linklen, $addon, $maxlen2 ];
            }
            else if( $groupId !== 0 )
            {
                if( $groupId === FAILED_GROUP )
                    $addon = '(failed)';
                else if( $groupId === ETHEREUM_TRANSFER_GROUP )
                    $addon = '(transfer)';
                else
                    w8_err( "unknown $groupId" );

                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . $addon . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );

                $tdb[$groupId] = [ $link, $linklen, $addon, $maxlen2 ];
            }
        }

        $wtype = TYPE_STRINGS[$type];
        $reclen += strlen( $wtype );
        $maxlen1 = max( $maxlen1, $reclen );
        $block = w8k2h( $ts[TXKEY] );

        if( $lastblock !== $block )
        {
            $lastblock = $block;
            $date = date( 'Y.m.d H:i', $RO->getTimestampByHeight( $block ) + $z * 60 );
        }

        if( $aid )
        {
            $otx = $isa && $type > 0;
            $bld = $otx || ( $isb && ( $type === TX_INVOKE || $type === ITX_INVOKE ) );
            $act = $otx ? '<small>&#183; ' : '<small>&nbsp; ';
            $tar = $isa ? ( $isb ? '<>' : w8io_a( $b ) ) : w8io_a( $a );
            $blk = $type > 0 || ( $isb && $type === ITX_INVOKE ) ? '&nbsp;&#183; </small><a href="' : '&nbsp;&nbsp; </small><a href="';

            {
                $txkey = '<a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . $date . '</a>';

                if( $aspam )
                    $fee .= ' <small>spam</small>';

                $outs[] = [
                    $act,
                    ( $bld ? '<b>' : '' ) . $act . $txkey . $blk .
                    W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a>' . $amount . $asset . ( $bld ? '</b>' : '' ),
                    $reclen,
                    $addon, $linklen,
                    $tar . $fee,
                ];
            }
        }
        else
        {
            if( isset( $amount[1] ) && $amount[1] === '-' )
                $amount = ' ' . substr( $amount, 2 );

            $fee = '';
            if( $aspam )
                $fee .= ' <small>spam</small>';

            echo
                '<small><a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . $date . '</a>' .
                ' <a href="' . W8IO_ROOT . 'b/' . $block . '">[' . $block . ']</a></small>' .
                ' <a href="' . W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a> ' . w8io_a( $a ) . ' > ' . w8io_a( $b ) .
                $addon . $amount . $asset . $fee . PHP_EOL;
        }
    }

    foreach( $outs as $out )
    {
        $act = $out[0];
        $p1 = $out[1];
        $p1pad = $out[2];
        $addon = $out[3];
        $p2pad = mb_strlen( html_entity_decode( $addon ), 'UTF-8' );
        $p3 = $out[5];

        if( $p2pad === 0 )
        {
            $p1pad = $maxlen1 - $p1pad + $maxlen2;
            $pad2 = '';
        }
        else
        {
            $p1pad = $maxlen1 - $p1pad;
            $p2pad = $maxlen2 - ( $p2pad - $out[4] );
            $pad2 = $p2pad > 3 ? ( ' <span>' . str_repeat( '—', $p2pad - 1 ) . '</span>' ) : str_repeat( ' ', $p2pad );
        }
        $pad1 = $p1pad > 3 ? ( ' <span>' . str_repeat( '—', $p1pad - 1 ) . '</span>' ) : str_repeat( ' ', $p1pad );

        echo $p1 . $pad1 . $addon . $pad2 . ' ' . $p3 . PHP_EOL;
    }

    if( isset( $lazy ) )
        echo $lazy;
}

function w8io_a( $address, $asset = null )
{
    if( isset( $address[5] ) && $address[5] === ':' )
        $address = substr( $address, 8 );
    $f = isset( $asset ) ? ( '/f/' . $asset ) : '';
    return '<a href=' . W8IO_ROOT . $address . $f . '>' . $address . '</a>';
}

function is_address( $value )
{
    return is_string( $value ) && strlen( $value ) === 35 && $value[0] === '3';
}

function w8io_height( $height )
{
    return '<a href=' . W8IO_ROOT . 'b/' . $height . '>' . $height . '</a>';
}

function w8io_txid( $txid, $tx )
{
    return '<a href=' . W8IO_ROOT . ( isset( $tx['assetPair'] ) ? 'o/' : 'tx/' ) . $txid . '>' . $txid . '</a>';
}

function htmlfilter( $kv )
{
    $fkv = [];
    foreach( $kv as $k => $v )
        if( is_array( $v ) )
        {
            $fkv[$k] = htmlfilter( $v );
        }
        else
        {
            if( is_string( $k ) )
                switch( $k )
                {
                    case 'id': $v = w8io_txid( $v, $kv ); break;
                    case 'sender':
                    case 'recipient':
                    case 'dApp':
                    case 'address':
                    case 'target': $v = w8io_a( $v ); break;
                    case 'attachment': $fkv[$k . '-decoded'] = htmlentities( trim( preg_replace( '/\s+/', ' ', wk()->base58Decode( $v ) ) ) );
                    default:
                        if( !isset( $v ) )
                            $v = null;
                        else
                        if( is_string( $v ) )
                        {
                            $v = htmlentities( $v );
                            if( is_address( $v ) )
                                $v = w8io_a( $v );
                        }
                }

            $fkv[$k] = $v;
        }

    return $fkv;
}

function htmlscript( $tx )
{
    $decompile = wk()->fetch( '/utils/script/decompile', true, $tx['script'] );
    if( $decompile === false )
        return;
    $decompile = wk()->json_decode( $decompile );
    if( $decompile === false )
        return;
    $decompile1 = $decompile['script'];

    require_once 'include/RO.php';
    $RO = new RO( W8DB );
    $a = $RO->getAddressIdByAddress( $tx['sender'] );
    if( $a === false )
        return;
    $txkey = $RO->getTxKeyByTxId( $tx['id'] );
    if( $tx['type'] === 15 )
    {
        $assetId = $RO->getIdByAsset( $tx['assetId'] );
        $prevScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 15 AND r5 = ' . $assetId . ' AND r1 < ' . $txkey . ' ORDER BY r0 DESC LIMIT 1' );
    }
    else if( $tx['type'] === 13 )
    {
        $prevScript = $RO->db->query( 'SELECT * FROM pts WHERE r3 = ' . $a . ' AND r2 = 13 AND r1 < ' . $txkey . ' ORDER BY r0 DESC LIMIT 1' );
    }
    else
        return;
    $r = $prevScript->fetchAll();
    if( !isset( $r[0][1] ) )
    {
        $decompile2 = '';
        $result = 'Previous script: none' . PHP_EOL . PHP_EOL;
    }
    else
    {
        $tx = $RO->getTxIdByTxKey( $r[0][1] );
        $tx = wk()->getTransactionById( $tx );
        $result = 'Previous script: ' . ( $tx === false ? 'ERROR' : w8io_txid( $tx['id'], $tx ) ) . PHP_EOL . PHP_EOL;

        if( empty( $tx['script'] ) )
            $decompile2 = '';
        else
        {
            $decompile = wk()->fetch( '/utils/script/decompile', true, $tx['script'] );
            if( $decompile === false )
                return;
            $decompile = wk()->json_decode( $decompile );
            if( $decompile === false )
                return;
            $decompile2 = $decompile['script'];
        }
    }

    if( $decompile1 === $decompile2 )
    {
        $result .= 'Diff: no diff';
    }
    else
    {
        $result .= '<style>' . \Jfcherng\Diff\DiffHelper::getStyleSheet() . '</style>';
        $result .= 'Diff: ' . PHP_EOL . \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'Inline', [], [ 'detailLevel' => 'word' ] ) . PHP_EOL;
        if( !empty( $decompile2 ) )
            $result .= 'Full: ' . PHP_EOL . \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'Inline', [ 'context' => \Jfcherng\Diff\Differ::CONTEXT_ALL ], [ 'detailLevel' => 'word' ] );
    }

    return $result;
}

if( $address === 'tx' && $f !== false )
{
    if( strlen( $f ) >= 32 )
    {
        prolog();
        require_once 'include/RO.php';
        $RO = new RO( W8DB );
        $txid = $RO->getTxKeyByTxId( $f );
        if( $txid === false )
            echo json_encode( [ 'error' => "getTxKeyByTxId( $f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        else
        {
            $tx = wk()->getTransactionById( $f );
            if( $tx === false )
                echo json_encode( [ 'error' => "getTransactionById( $f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
            else
            {
                w8io_print_transactions( false, 'r1 = ' . $txid, false, 1000, 'txs', 3 );

                if( !empty( $tx['script'] ) )
                    $addon = htmlscript( $tx );
                $tx = htmlfilter( $tx );
                echo '<br>' . json_encode( $tx, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
                if( isset( $addon ) )
                    echo PHP_EOL . PHP_EOL . $addon;
            }
        }
    }
}
else
if( $address === 'o' && $f !== false )
{
    if( strlen( $f ) >= 32 )
    {
        prolog();
        wk()->setNodeAddress( W8IO_MATCHER );
        $json = wk()->fetch( '/matcher/transactions/' . $f );
        if( $json === false || false === ( $json = wk()->json_decode( $json ) ) )
            echo json_encode( [ 'error' => "fetch( /matcher/transactions/$f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        else
        {
            require_once 'include/RO.php';
            $RO = new RO( W8DB );
            foreach( (array)$json as $tx )
            {
                $id = $tx['id'];
                $txid = $RO->getTxKeyByTxId( $id );
                if( $txid === false )
                    echo json_encode( [ 'error' => "getTxKeyByTxId( $id ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES ) . PHP_EOL;
                else
                    w8io_print_transactions( false, 'r1 = ' . $txid, false, 1000, 'txs', 3 );
            }

            $json = htmlfilter( $json );
            echo '<br>' . json_encode( $json, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        }
    }
}
else
if( $address === 'b' )
{
    prolog();
    $height = (int)$f;
    $block = wk()->getBlockAt( $height );

    if( $block === false )
    {
        echo 'block not found';
    }
    else
    {
        $txs = $block['transactions'];
        unset( $block['transactions'] );
        $block['generator'] = w8io_a( $block['generator'] );
        if( $height > 1 )
        {
            unset( $block['height'] );
            $block['previous'] = w8io_height( $height - 1 );
        }
        $block['height'] = w8io_height( $height );
        $block['next'] = w8io_height( $height + 1 );
        $ftxs = [];
        foreach( $txs as $tx )
            $ftxs[] = htmlfilter( $tx );
        $block['transactions'] = $ftxs;
        echo json_encode( $block, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
    }
}
else
if( $address === 'top' && isset( $info ) )
{
    prolog();
    echo '<pre>';
    w8io_print_distribution( $f, $aid, $info, (int)$arg );
    echo '</pre>';
}
else
if( $address === 'GENERATORS' )
{
    prolog();
    $arg = $arg !== false ? intval( $arg ) : null;

    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    $generators = $RO->getGenerators( $n, $arg );

    $Q = isset( $showtime ) ? 128 : 80;
    $infos = [];
    $gentotal = 0;
    $feetotal = 0;
    $blktotal = 0;

    foreach( $generators as $generator => $pts )
    {
        $balance = $RO->getBalanceByAddressId( $generator );
        $balance = ( isset( $balance[0] ) ? $balance[0] : 0 ) + ( isset( $balance[WAVES_LEASE_ASSET] ) ? $balance[WAVES_LEASE_ASSET] : 0 );
        //if( isset( $arg ) )
            //$balance = $api->correct_balance( $generator, $arg, $arg > WAVES_LEASE_ASSET ? $balance : null );
        $gentotal += $balance;

        foreach( $pts as $height => $amount )
        {
            if( !isset( $from ) || $from > $height )
                $from = $height;
            if( !isset( $to ) || $to < $height )
                $to = $height;
        }

        $infos[$generator] = array( 'balance' => $balance, 'pts' => $pts );
    }

    $fromtime = $RO->getTimestampByHeight( $from );
    $totime = $RO->getTimestampByHeight( $to );

    $q = $n / $Q;
    $qb = max( intdiv( (int)$q, 16 ), 5 );

    $period = $totime - $fromtime;
    $period = round( $period / 3600 );
    if( $period < 100 )
        $period = $period . ' h';
    else
        $period = round( $period / 24 ) . ' d';

    $totime = date( 'Y.m.d H:i', $totime + $z * 60 );

    if( isset( $showtime ) )
    {
        $highlights = [];
        $hs = intdiv( $from, 10000 );
        $he = intdiv( $to, 10000 );

        if( $hs * 10000 == $from )
            $highlights[] = 0;
        if( 0 && $from === 1 )
            $highlights[] = $Q - 1 - intdiv( $to - 1, $q );
        while( $he !== $hs )
            $highlights[] = $Q - 1 - intdiv( $to - ( ++$hs * 10000 ), $q );

        $topad = str_pad( $to, 7, ' ', STR_PAD_LEFT );
        $periodpad = str_pad( $period, 4, ' ', STR_PAD_LEFT );
        echo "GENERATORS ( ~ $periodpad ) @ <b>$topad</b> <small>($totime)</small><hr>";
    }
    else
    {
        echo "GENERATORS ( ~ $period ) @ $to <small>($totime)</small><hr>";
    }

    $generators = $infos;
    uasort( $generators, function( $a, $b ){ return( $a['balance'] < $b['balance'] ? 1 : -1 ); } );

    $n = 0;
    foreach( $generators as $id => $generator )
    {
        $address = $RO->getAddressById( $id );
        $alias = $RO->getFirstAliasById( $id );
        $padlen = max( 30 - strlen( $alias ), 0 );

        $address = '<a href="' . W8IO_ROOT . "$address\">$address</a>";
        $alias = $alias === false ? ' ' : ' <a href="' . W8IO_ROOT . "$alias\">$alias</a>";
        $alias .= str_pad( '', $padlen );

        $balance = $generator['balance'];
        $percent = str_pad( number_format( $gentotal ? ( $balance / $gentotal * 100 ) : 0, 2, '.', '' ) . '%', 7, ' ', STR_PAD_LEFT );
        $balance = str_pad( number_format( $balance / 100000000, 0, '', "'" ), 10, ' ', STR_PAD_LEFT );

        $pts = $generator['pts'];
        $count = count( $pts );
        $blktotal += $count;

        $matrix = array_fill( 0, $Q, 0 );
        $fee = 0;
        foreach( $pts as $block => $amount )
        {
            $target = $Q - 1 - (int)floor( ( $to - $block ) / $q );
            $matrix[$target]++;
            $fee += $amount;
        }

        $feetotal += $fee;
        $fee = w8io_amount( $fee, 8, 14 );

        $mxprint = '';
        for( $i = 0; $i < $Q; $i++ )
        {
            $blocks = $matrix[$i];
            if( $blocks === 0 )
                $blocks = '-';
            elseif( $blocks === 1 )
                $blocks = 'o';
            elseif( $blocks <= $qb )
                $blocks = 'O';
            else
                $blocks = '<b>O</b>';
            if( isset( $highlights ) && in_array( $i, $highlights ) )
                $blocks = "<a style=\"background-color: #384038;\">$blocks</a>";
            $mxprint .= $blocks;
        }

        if( isset( $showtime ) && $n > 64 )
        {
            $n++;
            continue;
        }

        echo str_pad( ++$n, isset( $showtime ) ? 4 : 3, ' ', STR_PAD_LEFT ) . ") $address $alias $balance $percent  $mxprint $fee ($count)" . PHP_EOL;
    }

    $ntotal = str_pad( isset( $showtime ) ? $n : '', isset( $showtime ) ? 4 : 3, ' ', STR_PAD_LEFT );
    $gentotal = str_pad( number_format( $gentotal / 100000000, 0, '', "'" ), 79, ' ', STR_PAD_LEFT );
    $feetotal = str_pad( number_format( $feetotal / 100000000, 8, '.', '' ), ( isset( $showtime ) ? 48 : 0 ) + 104, ' ', STR_PAD_LEFT );

    echo "<small style=\"font-size: 50%;\"><br></small><b>$ntotal $gentotal $feetotal</b> ($blktotal)" .  PHP_EOL;

    if( isset( $showtime ) )
        for( $i = $n; $i <= 64; $i++ )
            echo PHP_EOL;
}
else
if( $address === 'ACTIVATION' )
{
    prolog();
    if( $f !== false )
    {
        $f = intval( $f );
        $addon = '/ ' . $f . ' ';
    }
    else
    {
        $f = false;
        $addon = '';
    }

    require_once 'include/RO_headers.php';
    $RO = new RO_headers;

    $hi = $RO->db->getHigh( 0 );
    $headers = $RO->kv->getValueByKey( $hi );
    $height = $headers['height'];

    echo "ACTIVATION $addon<hr>";

    if( $f === false )
    {
        $activation = wk()->fetch( '/activation/status' );
        if( $activation === false )
            exit( 'offline' );

        $activation = wk()->json_decode( $activation );
        $json = htmlfilter( $activation );
        $output = json_encode( $json, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        $output = str_replace( W8IO_ROOT . 'tx', W8IO_ROOT . 'ACTIVATION', $output );
        echo $output;
    }
    else
    {
        $cache_file = W8IO_DB_DIR . 'votingInterval';
        if( file_exists( $cache_file ) )
            $votingInterval = intval( file_get_contents( $cache_file ) );
        else
        {
            $activation = wk()->fetch( '/activation/status' );
            if( $activation === false )
                exit( 'offline' );

            $activation = wk()->json_decode( $activation );
            $votingInterval = $activation['votingInterval'];
            file_put_contents( $cache_file, $votingInterval );
        }

        $period_number = $arg !== false ? intval( $arg ) : intdiv( $height, $votingInterval );

        $period_start = $period_number * $votingInterval + 1;
        $period_end = $period_start + $votingInterval - 1;

        $tt_start = $RO->kv->getValueByKey( $period_start )['timestamp'] ?? false;
        $tt_end = $RO->kv->getValueByKey( $period_end )['timestamp'] ?? false;

        $past_blocks = 10000;
        $tt_past = $RO->kv->getValueByKey( $height - $past_blocks )['timestamp'] ?? false;
        if( $tt_past !== false )
        {
            $tt_now = $headers['timestamp'];
            $block_speed = intdiv( $tt_now - $tt_past, $past_blocks );
            if( $tt_start === false )
            {
                $blocks_left = $period_start - $height;
                $tt_left = $blocks_left * $block_speed;
                $tt_start = $tt_now + $tt_left;
            }
            if( $tt_end === false )
            {
                $blocks_left = $period_end - $height;
                $tt_left = $blocks_left * $block_speed;
                $tt_end = $tt_now + $tt_left;
            }
        }

        $period_dates = $tt_start === false ? '' :
            ( ' (' . date( 'Y.m.d H:i', intdiv( $tt_start, 1000 ) + $z * 60 ) . ' .. '
                   . date( 'Y.m.d H:i', intdiv( $tt_end, 1000 ) + $z * 60 ) . ')' );

        $link_prolog = '<a href="' . W8IO_ROOT . 'ACTIVATION/' . $f . '/';
        echo '    previous: ' . $link_prolog . ( $period_number - 1 ) . '">' . ( $period_start - $votingInterval ) . ' .. ' . ( $period_end - $votingInterval ) . '</a>' . PHP_EOL;
        echo '     current: ' . $link_prolog . ( $period_number + 0 ) . '">' . ( $period_start ) . ' .. ' . ( $period_end ) . '</a>' . $period_dates . PHP_EOL;
        echo '        next: ' . $link_prolog . ( $period_number + 1 ) . '">' . ( $period_start + $votingInterval ) . ' .. ' . ( $period_end + $votingInterval ) . '</a>' . PHP_EOL;
        echo '      height: ' . $height . PHP_EOL;
        $tt_left = $tt_left ?? 0;
        $days = intdiv( $tt_left, 1000 * 3600 * 24 );
        $tt_days = $days * 1000 * 3600 * 24;
        $hours = intdiv( $tt_left - $tt_days, 1000 * 3600 );
        $tt_hours = $hours * 1000 * 3600;
        $minutes = intdiv( $tt_left - $tt_days - $tt_hours, 1000 * 60 );
        echo '        left: ' . $days . 'd ' . $hours . 'h '. $minutes . 'm' . PHP_EOL . PHP_EOL;

        //foreach( $activation['features'] as $feature )
        {
            //if( intval( $feature['id'] ) === $f )
            {
                $votes = [];
                $totals = [];
                $lasts = [];
                $count = 0;
                if( $period_start === 0 )
                    $period_start = 1;
                for( $i = $period_start; $i <= $period_end && $i <= $height; ++$i )
                {
                    $headers = $RO->kv->getValueByKey( $i );
                    $generator = $headers['generator'] ?? '';
                    $features = $headers['features'] ?? [];
                    $totals[$generator] = 1 + ( $totals[$generator] ?? 0 );
                    if( in_array( $f, $features ) )
                    {
                        $votes[$generator] = 1 + ( $votes[$generator] ?? 0 );
                        $lasts[$generator] = true;
                        ++$count;
                    }
                    else
                        $lasts[$generator] = false;
                }

                if( $i > $period_start )
                {
                    $weight = 0;
                    $blocks = $i - $period_start;
                    foreach( $totals as $generator => $total )
                    {
                        if( $lasts[$generator] )
                            $weight += $total / $blocks;
                    }

                    $blocks_done = $i - $period_start;
                    $blocks_total = $period_end - $period_start + 1;
                    $blocks_left = $blocks_total - $blocks_done;
                    $addon = $blocks_left * $weight;
                    $forecast = (int)( $count + $addon );
                    $current_percent = intdiv( 10000 * $count, $blocks_done );
                    $forecast_percent = intdiv( 10000 * $forecast, $blocks_total );
                    $future_blocks = (int)( $votingInterval * $weight );
                    $future_percent = intdiv( 10000 * $future_blocks, $votingInterval );

                    echo '     SUPPORT: ' . w8io_amount( $current_percent, 2, 6, false ) . '% <small>(' . $count . '/' . $blocks_done . ')</small>' . PHP_EOL;
                    echo '    FORECAST: ' . w8io_amount( $forecast_percent, 2, 6, false ) . '% <small>(' . $forecast . '/' . $blocks_total . ')</small>' . PHP_EOL;
                    echo '      FUTURE: ' . w8io_amount( $future_percent, 2, 6, false ) . '% <small>(' . $future_blocks . '/' . $votingInterval . ')</small>' . PHP_EOL . PHP_EOL;

                    arsort( $totals );

                    foreach( $totals as $generator => $total )
                    {
                        $vote = ( $votes[$generator] ?? 0 );
                        $percent = intdiv( 10000 * $vote, $total );
                        $percent_total = intdiv( 10000 * $vote, $blocks_done );
                        $last = $lasts[$generator] ? '&#183;' : ' ';
                        echo '    ' . w8io_a( $generator ) . ': ' . w8io_amount( $percent_total, 2, 6, false ) . '% ' . $last . ' <small>'. w8io_amount( $percent, 2, 6, false ) .'% (' . $vote . '/' . $total . ')</small>' . PHP_EOL;
                    }
                }
            }
        }
    }
}
else if( $f === 'data' )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    $aid = $RO->getAddressIdByString( $address );
    if( $aid === false )
        exit( 'unknown address' );

    $data = [];
    if( $arg !== false ) // less filter for data keys
    {
        $urio = explode( '/', $urio );
        $arg = $urio[2];
        if( $arg2 === false )
        {
            $key = urldecode( $arg );
            $kid = $RO->getIdByKey( $key );
            if( $kid !== false )
            {
                $value = $RO->getValueByAddressKey( $aid, $kid );
                if( $value !== false )
                {
                    [ $r0, $r1, $r2, $r3, $r4, $r5, $r6, $r7 ] = $value;
                    if( $r6 !== TYPE_NULL )
                    {
                        $value = $RO->getValueByTypeId( $r6, $r5 );
                        $data[$key] = [ 'key' => $key, 'type' => DATA_TYPE_STRINGS[$r6], 'value' => $value ];
                    }
                }
            }
        }
        else
        {
            $arg2 = $urio[3];
            $arg3 = $arg3 === false ? false : $urio[4];
        }
    }
    else
    {
        [ $data, $lazy ] = w8io_get_data( $address, $aid, PHP_INT_MAX, 1000 );
    }

    prolog();
    $datauri = '<a href="' . W8IO_ROOT . $address . '/data/';
    echo '<a href="' . W8IO_ROOT . $address . '">' . $address . '</a> &#183; ' . $datauri . '">data</a>';
    if( $arg !== false )
        echo ' &#183; ' . $datauri . $arg . '">' . htmlentities( urldecode( $arg ) ) . '</a>';
    if( $arg2 !== false )
        echo ' &#183; ' . $datauri . $arg2 . '">' . htmlentities( urldecode( $arg2 ) ) . '</a>';
    if( $arg3 !== false )
        echo ' &#183; ' . $datauri . $arg3 . '">' . htmlentities( urldecode( $arg3 ) ) . '</a>';
    echo '<br>' . PHP_EOL . '<pre>{';
    w8io_print_data( $datauri, $data, $lazy ?? false );
    echo PHP_EOL . '</pre>';
}
else
{
    if( !isset( $RO ) )
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );
    }

    $aid = $RO->getAddressIdByString( $address );

    $where = false;
    $d = 3; // 0 - ?; 1 - i; 2 - o; 3 - io;
    $filter = 0;

    if( !empty( $f ) )
    if( $arg !== false )
    {
        if( $f[0] === 'f' )
        {
            {
                if( $arg === 'Waves' )
                    $arg = 0;
                else
                if( is_numeric( $arg ) )
                {
                    $arg = $RO->getAssetById( $arg );
                    if( $arg === false )
                        exit( 'unknown asset' );
                    exit( header( 'location: ' . W8IO_ROOT . $address . '/f/' . $arg ) );
                }
                else
                {
                    $fasset = $arg;
                    $arg = $RO->getIdByAsset( $arg );
                    if( $arg === false )
                        exit( 'unknown asset' );
                }
            }

            $filter = 1;
            $where = "r5 = $arg";

            if( isset( $f[1] ) )
            {
                if( $f[1] === 'i' )
                {
                    $d = 1;
                }
                else
                if( $f[1] === 'o' )
                {
                    $d = 2;
                }
            }
        }
        else
        if( $f[0] === 't' )
        {
            if( !is_numeric( $arg ) )
                exit( 'unknown type' );

            $filter = 2;
            $where = 'r2 = ' . $arg;

            if( isset( $f[1] ) )
            {
                if( $f[1] === 'i' )
                {
                    $d = 1;
                }
                else
                if( $f[1] === 'o' )
                {
                    $d = 2;
                }
            }
        }
        else if( $aid === false && $f === 'g' )
        {
            if( is_numeric( $arg ) )
            {
                if( $arg === '-1' )
                    $arg = 'failed';
                else
                if( $arg === '-2' )
                    $arg = 'ethereum_transfer';
                else
                {
                    $arg = $RO->getGroupById( (int)$arg );
                    if( $arg === false )
                        exit( 'unknown group' );
                    $first = $arg[0];
                    if( $first === '>' || $first === '<' )
                    {
                        $sep = strpos( $arg, ':' );
                        $asset1 = (int)substr( $arg, 1, $sep - 1 );
                        $asset2 = (int)substr( $arg, $sep + 1 );
                        $asset1 = $asset1 === WAVES_ASSET ? 'WAVES' : $RO->getAssetById( $asset1 );
                        $asset2 = $asset2 === WAVES_ASSET ? 'WAVES' : $RO->getAssetById( $asset2 );
                        $arg = ( $first === '>' ? '1_' : '2_' ) . $asset1 . '_' . $asset2;
                    }
                    else
                    {
                        $args = explode( ':', $arg );
                        $dapp = $RO->getAddressById( $args[0] );
                        $function = $RO->getFunctionById( $args[1] );
                        $arg = $dapp . '_' . $function . '_' . $args[2];
                    }
                }
                exit( header( 'location: ' . W8IO_ROOT . 'txs/g/' . $arg ) );
            }

            if( $arg === 'failed' )
                $arg = FAILED_GROUP;
            else
            if( $arg === 'ethereum_transfer' )
                $arg = ETHEREUM_TRANSFER_GROUP;
            else
            {
                $args = explode( '_', $arg );
                $group = $args[0];
                if( $group === '1' || $group === '2' )
                {
                    if( !isset( $args[1] ) || !isset( $args[2] ) )
                        exit( 'not enough assets' );

                    if( $args[1] === 'WAVES' )
                    {
                        $asset1 = WAVES_ASSET;
                    }
                    else
                    {
                        $asset1 = $RO->getIdByAsset( $args[1] );
                        if( $asset1 === false )
                            exit( 'unknown asset1' );
                    }

                    if( $args[2] === 'WAVES' )
                    {
                        $asset2 = WAVES_ASSET;
                    }
                    else
                    {
                        $asset2 = $RO->getIdByAsset( $args[2] );
                        if( $asset2 === false )
                            exit( 'unknown asset2' );
                    }

                    $group = ( $group === '1' ? '>' : '<' ) . $asset1 . ':' . $asset2; // getGroupExchange
                }
                else
                {
                    $dapp = $RO->getAddressIdByString( $group );
                    if( $dapp === false )
                        exit( 'unknown dapp' );

                    $type = end( $args );
                    if( !is_numeric( $type ) )
                        exit( 'bad type' );

                    $function = substr( $arg, strlen( $group ) + 1, -1 - strlen( $type ) );
                    $function = $RO->getFunctionByName( $function );
                    if( $function === false )
                        exit( 'unknown function' );

                    $group = $dapp . ':' . $function . ':' . $type; // getGroupFunction
                }

                $arg = $RO->getGroupByName( $group );
                if( $arg === false )
                    exit( 'unknown group' );
            }

            $where = "r10 = $arg";
        }
    }
    else
    if( $aid !== false )
    {
        if( $f === 'i' )
        {
            $where = 'io';
            $d = 1;
        }
        else
        if( $f === 'o' )
        {
            $where = 'io';
            $d = 2;
        }
    }

    prolog();
    if( $aid === false )
    {
        echo '<pre>';
        w8io_print_transactions( false, $where, false, 100, $address, $d );
        echo '</pre>';
    }
    else
    {
        if( $aid <= 0 || strlen( $address ) !== 35 )
            $full_address = $RO->getAddressById( $aid );
        else
            $full_address = $address;
        $balance = $RO->getBalanceByAddressId( $aid );

        if( $balance === false )
            $balance = [ 0 => 0 ];

        //$heightTime = $RO->getLastHeightTimestamp();
        //$time = date( 'Y.m.d H:i', $heightTime[1] + $z * 60 );
        //$height = $heightTime[0];

        {
            $full_address_html = $full_address !== $address ? ( ' / <a href="' . W8IO_ROOT . $full_address . '">' . $full_address . '</a>' ) : '';
            //echo "<a href=\"". W8IO_ROOT . $address ."\">$address</a>$full_address @ $height <small>($time) ";
            echo '<a href="' . W8IO_ROOT . $address . '">' . $address . '</a>' . $full_address_html . ' <small>&#183; ';
            echo '<a href="' . W8IO_ROOT . $address . '/i">i</a><a href="' . W8IO_ROOT . $address . '/o">o</a> &#183; ';

            $out = '';
            $data = false;
            for( $t = -16; $t <= 19; ++$t )
            {
                $ti = asset_in( $t );
                $ti = $balance[$ti] ?? 0;
                $to = asset_out( $t );
                $to = $balance[$to] ?? 0;
                if( $ti > 0 || $to > 0 )
                {
                    if( $out !== '' )
                        $out .= ' &#183; ';
                    if( $t === TX_DATA )
                    {
                        $out .= '<a href="' . W8IO_ROOT . $address . '/data">data</a>&#183;';
                        $data = true;
                    }
                    else
                    {
                        if( $t === TX_SMART_ACCOUNT && $data === false )
                            $out .= '<a href="' . W8IO_ROOT . $address . '/data">data</a> &#183; ';
                        $out .= '<a href="' . W8IO_ROOT . $address . '/t/' . $t . '">' . TYPE_STRINGS[$t] . '</a>&#183;';
                    }
                    if( $ti > 0 )
                        $out .= '<a href="' . W8IO_ROOT . $address . '/ti/' . $t . '">i' . $ti . '</a>';
                    if( $to > 0 )
                        $out .= ( $ti > 0 ? '&#183;' : '' ) . '<a href="' . W8IO_ROOT . $address . '/to/' . $t . '">o' . $to . '</a>';
                }
            }
            echo $out . '</small>';
            echo PHP_EOL . PHP_EOL . '<table><tr><td valign="top"><pre>';
        }

        $tickers = [];
        $unlisted = [];

        if( !isset( $balance[0] ) )
            $balance[0] = 0;
        if( $filter === 1 && !isset( $balance[$arg] ) )
            $balance[$arg] = 0;

        $weights = [];
        $prints = [];

        // WAVES
        {
            $asset = "Waves";
            $amount = w8io_amount( $balance[0], 8 );
            $furl = W8IO_ROOT . $address . '/f/Waves';

            if( $arg === 0 && $filter === 1 )
            {
                echo '<b>' . $amount . ' <a href="' . W8IO_ROOT . 'top/Waves">' . $asset . '</a></b>';
                echo ' <small><a href="' . W8IO_ROOT . $address . '/fi/Waves">i</a><a href="' . W8IO_ROOT . $address . '/fo/Waves">o</a></small>' . PHP_EOL;
                echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
            }
            else
            {
                $weights[WAVES_ASSET] = 10000;
                $prints[WAVES_ASSET] = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
            }
        }

        if( isset( $balance[WAVES_LEASE_ASSET] ) )
        {
            $amount = $balance[WAVES_LEASE_ASSET] + ( isset( $balance[0] ) ? $balance[0] : 0 );

            if( $balance[0] !== $amount )
            {
                $asset = "Waves (GENERATOR)";
                $amount = w8io_amount( $amount, 8 );

                $weights[WAVES_LEASE_ASSET] = 1000;
                $prints[WAVES_LEASE_ASSET] = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
            }
        }

        foreach( $balance as $asset => $amount )
        {
            if( $amount === 0 && ( $asset !== $arg || $filter !== 1 ) )
                continue;

            if( $asset > 0 )
            {
                $info = $RO->getAssetInfoById( $asset );
                $weight = ord( $info[1] );
                if( $weight === 1 && $arg !== $asset )
                    continue;

                $id = $asset;
                $b = $asset === $arg && $filter === 1;
                $decimals = $info[0];
                if( $decimals === 'N' )
                {
                    $decimals = 0;
                    $weight = -1;
                }
                else
                {
                    $decimals = (int)$decimals;
                }
                $asset = substr( $info, 2 );
                $amount = w8io_amount( $amount, $decimals );

                $furl = W8IO_ROOT . $address . '/f/' . $id;

                $record = [ 'id' => $id, 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];

                if( $b )
                    $frecord = $record;
                else
                {
                    //if( $weight === -1 )
                        //continue;
                    $weights[$id] = $weight;
                    $prints[$id] = $record;
                }
            }
        }

        if( isset( $frecord ) )
        {
            echo '<b>' . $frecord['amount'] . ' <a href="' . W8IO_ROOT . 'top/' . $frecord['id'] . '">' . $frecord['asset'] . '</a></b>';
            echo ' <small><a href="' . W8IO_ROOT . $address . '/fi/' . $fasset . '">i</a><a href="' . W8IO_ROOT . $address . '/fo/' . $fasset . '">o</a></small>' . PHP_EOL;
            echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
        }

        arsort( $weights );

        foreach( $weights as $asset => $weight )
        {
            if( $weight <= 0 && !isset( $zerotrades ) )
            {
                echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
                $zerotrades = true;
            }

            $record = $prints[$asset];
            echo $record['amount'] . ' <a href="' . $record['furl'] . '">' . $record['asset'] . '</a>' . PHP_EOL;
        }

        if( !isset( $zerotrades ) )
        {
            echo '<span>' . str_repeat( '—', 39 ) . '&nbsp;</span>' .  PHP_EOL;
            $zerotrades = true;
        }

        echo '</pre></td><td valign="top"><pre>';

        if( $f === 'pay' )
        {
            $from = $arg;
            $to = $arg2;

            $incomes = $RO->getLeasingIncomes( $aid, $from, $to );

            if( $incomes !== false )
            for( ;; )
            {
                arsort( $incomes );

                if( $arg3 === 'raw' )
                {
                    echo "raw income ($from .. $to):" . PHP_EOL . PHP_EOL;

                    $raw = [];
                    foreach( $incomes as $a => $p )
                    {
                        $address = $RO->getAddressById( $a );
                        $p = number_format( $p, 14, '.', '' );
                        $raw[$address] = $p;
                    }

                    echo json_encode( $raw, JSON_PRETTY_PRINT );
                    break;
                }

                $percent = (int)$arg3;
                $percent = ( $percent > 0 && $percent < 100 ) ? $percent : 100;

                // waves_fees
                $waves_blocks = 0;
                $waves_fees = 0;

                $start_uid = $RO->db->query( 'SELECT * FROM pts WHERE r1 = ' . w8h2kg( $from ) )->fetchAll();
                if( isset( $start_uid[0][UID] ) )
                    $start_uid = (int)$start_uid[0][UID];
                else
                    break;

                $end_uid = $RO->db->query( 'SELECT * FROM pts WHERE r1 = ' . w8h2kg( $to ) )->fetchAll();
                if( isset( $end_uid[0][UID] ) )
                    $end_uid = (int)$end_uid[0][UID];
                else
                    $end_uid = PHP_INT_MAX;

                $query = $RO->db->query( "SELECT * FROM pts WHERE r0 >= $start_uid AND r0 <= $end_uid AND r4 = $aid AND r2 = 0" );
                foreach( $query as $ts )
                {
                    if( (int)$ts[ASSET] === 0 )
                    {
                        $waves_fees += (int)$ts[AMOUNT];
                        $waves_blocks++;
                    }
                }
                $waves_fees = intval( $waves_fees * $percent / 100 );

                echo "pay ($from .. $to) ($percent %):" . PHP_EOL . PHP_EOL;
                echo w8io_amount( $waves_blocks, 0 ) . ' Blocks' . PHP_EOL;
                echo w8io_amount( $waves_fees, 8 ) . " Waves" . PHP_EOL;

                $payments = [];
                foreach( $incomes as $a => $p )
                    if( $p * $waves_fees > 10000 )
                        $payments[] = $a;

                $reserve = count( $payments );
                $reserve = ( intdiv( $reserve, 100 ) + 1 ) * 100000 + $reserve * 50000 + ( $reserve % 2 ) * 50000;
                $waves_fees -= $reserve * 2;

                echo PHP_EOL;
                $waves = 0;
                $m = 0;
                $n = 0;

                foreach( $payments as $a )
                {
                    $p = $incomes[$a];

                    if( $n === 0 )
                    {
                        $m++;
                        echo "    Mass (Waves) #$m:" . PHP_EOL;
                        echo "    ------------------------------------------------------------" . PHP_EOL;
                    }
                    $address = $RO->getAddressById( $a );
                    $pay = number_format( $p * $waves_fees / 100000000, 8, '.', '' );
                    $waves += $pay;
                    echo "    $address, $pay" . PHP_EOL;
                    if( ++$n === 100 )
                    {
                        $n = 0;
                        echo "    ------------------------------------------------------------" . PHP_EOL . PHP_EOL;
                    }
                }

                if( $n )
                    echo "    ------------------------------------------------------------" . PHP_EOL . PHP_EOL;

                break;
            }
        }
        else
            w8io_print_transactions( $aid, $where, false, 100, $address, $d );
    }
}

if( !isset( $L ) )
{
    prolog();
}

echo '</pre></td></tr></table>';
echo '<hr><div width="100%" align="right"><pre><small>';
echo "<a href=\"https://github.com/deemru/w8io\">github/deemru/w8io</a>";
if( file_exists( '.git/FETCH_HEAD' ) )
{
    $rev = file_get_contents( '.git/FETCH_HEAD', false, null, 0, 40 );
    echo "/<a href=\"https://github.com/deemru/w8io/commit/$rev\">" . substr( $rev, 0, 7 ) . '</a> ';
}
if( !isset( $showtime ) )
{
    echo PHP_EOL . sprintf( '%.02f ms ', 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] ) );
    echo '<a id="L" href="" style="text-decoration: none;">' . ( $L ? '&#9680' : '&#9681' ) . '</a> ';
    if( defined( 'W8IO_ANALYTICS' ) )
        echo PHP_EOL . PHP_EOL . W8IO_ANALYTICS . ' ';
}
echo '</small></pre></div>
</pre>
    </body>
</html>';
if( isset( $showtime ) )
{
    file_put_contents( $showfile, ob_get_contents() );
    ob_end_clean();
    exit( file_get_contents( $showfile ) );
}
