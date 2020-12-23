<?php

namespace w8io;

use w8io_api;

require_once 'config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = 'w8io/pay/2200000/22630000';

$js = false;

$uri = explode( '/', preg_filter( '/[^a-zA-Z0-9_.@\-\/]+/', '', $uri . chr( 0 ) ) );

$address = $uri[0];
$f = isset( $uri[1] ) ? $uri[1] : false;
$arg = isset( $uri[2] ) ? $uri[2] : false;
$arg2 = isset( $uri[3] ) ? $uri[3] : false;
$arg3 = isset( $uri[4] ) ? $uri[4] : false;
$arg4 = isset( $uri[5] ) ? $uri[5] : false;

if( empty( $address ) )
    $address = 'GENERATORS';

$light = ( isset( $_COOKIE['light'] ) && (bool)$_COOKIE['light'] ) ? true : false;
if( $address === 'ld' )
{
    $address = '';
    $light = !$light;
    setcookie( 'light', $light, 0x7FFFFFFF, '/' );
}
else
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
else
if( $address === 'api' )
{
    if( strlen( $f ) > 20 )
    {
        $wk = wk();
        if( false === ( $f = $wk->base58Decode( $f ) ) ||
            false === ( $call = $wk->decryptash( $f ) ) ||
            false === ( $call = $wk->json_decode( $call ) ) )
            exit( $wk->log( 'e', 'bad API call' ) );

        switch( $call['f'] )
        {
            case 't':
            {
                require_once 'include/RO.php';
                $RO = new RO( W8DB );
                
                $aid = $call['i'];
                $where = $call['w'];
                $uid = $call['u'];
                $address = $call['a'];

                echo '<pre>';
                w8io_print_transactions( $aid, $where, $uid, 100, $address, false === strpos( $where, 'r5' ) );
                echo '</pre>';
                return;
            }
        }

        exit( $wk->log( 'e', 'bad API call' ) );
    }
    if( $f === 'chart' )
    {
        require_once './include/w8io_base.php';
        require_once './include/w8io_api.php';
        $api = new w8io_api();

        $from = (int)$arg;
        $to = (int)$arg2;

        if( $from > $to )
            exit;

        $height = $api->get_height();
        $to = min( $height, $to );
        $Q = 1;

        while( ( $to - $from ) / $Q > 1000 && $Q < 10000 )
            $Q *= 10;

        $from -= $from % $Q;
        $dataset = $api->get_dataset( $Q, $from, $to );

        $out = 'block';
        $total = $dataset['totals']['txs'];
        $out .= ", total ($total)";

        $types = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, W8IO_TYPE_INVOKE_DATA, W8IO_TYPE_INVOKE_TRANSFER ];
        $etypes = [];
        foreach( $types as $i )
        {
            $name = w8io_tx_type( $i );
            if( isset( $dataset['totals'][$i] ) )
            {
                $total = $dataset['totals'][$i];
                $out .= ", $name ($total)";
                $etypes[] = $i;
            }
        }
        $types = $etypes;

        $out .= PHP_EOL;

        foreach( $dataset['txs'] as $key => $value )
        {
            $tvalue = $value;
            $out .= "$key, $tvalue";
            foreach( $types as $i )
                if( isset( $dataset[$i][$key] ) )
                {
                    $tvalue = $dataset[$i][$key];
                    $out .= ", $tvalue";
                }
                else
                {
                    $out .= ", 0";
                }
            
            $out .= PHP_EOL;
        }

        echo $out;
    }
    
    exit;
}

if( $f === 'f' )
{
    if( $arg === 'Waves' )
        $arg = 0;
    else
    if( is_numeric( $arg ) )
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );

        $arg = $RO->getAssetById( $arg );
        if( $arg === false )
            w8io_error( 'unknown asset' );
        exit( header( 'location: ' . W8IO_ROOT . $address . '/f/' . $arg ) );
    }
    else
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );

        $arg = $RO->getIdByAsset( $arg );
        if( $arg === false )
            w8io_error( 'unknown asset' );
    }
}
elseif( $arg !== false )
    $arg = (int)$arg;

if( $light )
{
    $bcolor = 'FFFFFF';
    $tcolor = '202020';
    $hcolor = '000000';
}
else
{
    $bcolor = '404840';
    $tcolor = 'BABECA';
    $hcolor = 'DADEFA';
}

if( !$js )
echo sprintf( '
<!DOCTYPE html>
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="format-detection" content="telephone=no">
        <meta name="format-detection" content="date=no">
        <meta name="format-detection" content="address=no">
        <meta name="format-detection" content="email=no">
        <title>w8io%s</title>
        <link rel="shortcut icon" href="%sstatic/favicon.ico" type="image/x-icon">
        <link rel="stylesheet" href="%sstatic/fonts.css">
        <script type="text/javascript" src="%sstatic/jquery.js" charset="UTF-8"></script>
<script>
$(document).ready( function()
{
    g_loading = false;
    g_lazyload = $(".lazyload");
    if( g_lazyload.length )
    {
        $(window).scroll( lazyload );
        lazyload();
    }
} );

function lazyload()
{
    if( g_loading )
        return;

    var wt = typeof this.scrollY !== "undefined" ? this.scrollY : $(window).scrollTop();
    var wb = wt + this.innerHeight;
    var ot = g_lazyload.offset().top;
    var ob = ot + g_lazyload.height();

    if( wt <= ob && wb >= ot )
    {
        g_loading = true;
        $.get( g_lazyload.attr( "url" ), null, function( data )
        {
            if( data )
            {
                $( data ).insertAfter( g_lazyload );
                g_lazyload.remove();

                g_lazyload = $(".lazyload");
                if( g_lazyload.length )
                {
                    g_loading = false;
                    return lazyload();
                }
            }

            $(window).off( "scroll", lazyload );
        } );
    }
}
</script>
<style>
    body, table
    {
        font-size: 14pt; font-family: Inconsolata, monospace;
        background-color: #%s;
        color: #%s;
        border-collapse: collapse;
        overflow-y: scroll;%s
    }
    pre
    {
        font-family: Inconsolata, monospace;
        font-style: normal;
        font-weight: 400;
        font-stretch: 100%%;
        font-size: 14pt;
        margin: 0;
        unicode-bidi: bidi-override;
    }
    small
    {
        font-size: 10pt;
    }
    a
    {
        color: #%s;%s
        text-decoration: none;
        border-bottom: 1px dotted #606870;
        
    }
    a:hover
    {
        border-bottom: 1px solid #%s;
    }
    hr
    {
        margin: 1em 0 1em 0;
        height: 1px;
        border: 0;
        background-color: #606870;
    }
</style>
    </head>
    <body>
        <pre>
', empty( $address ) ? '' : " / $address", W8IO_ROOT, W8IO_ROOT, W8IO_ROOT,
//isset( $showtime ) ? '0.66vw' : '14pt',
$bcolor, $hcolor,
isset( $showtime ) ? 'margin: 1em 2em 1em 2em; filter: brightness(144%);' : '',
$tcolor,
isset( $showtime ) ? 'text-decoration: none;' : '',
$tcolor );

function w8io_print_distribution( $assetId )
{
    global $api;

    if( $assetId )
    {
        $info = $api->get_asset_info( $assetId );
        $decimals = $info['decimals'];
        $name = $info['name'];
    }
    else
    {
        $decimals = 8;
        $name = 'Waves';
    }
    
    $balances = $api->get_asset_distribution( $assetId );
    $total = 0;
    foreach( $balances as $balance )
    {
        $aid = (int)$balance[0];
        if( $aid > 0 )
        {
            $amount = (int)$balance[1];
            if( $amount === 0 )
                break;
            $address = $api->get_address( $aid );
            $total += $amount;
            $amount = w8io_amount( $amount, $decimals );
            echo '<a href="' . W8IO_ROOT . $address . '\">' . $address . '</a>: ' . $amount . PHP_EOL;
        }    
    }
}

function w8io_sign( $sign )
{
    if( $sign > 0 )
        return '+';
    if( $sign < 0 )
        return '-';
    return '';
}

function w8io_print_transactions( $aid, $where, $uid, $count, $address, $spam = true )
{
    global $RO;
    global $REST;
    global $js;
    
    $pts = $RO->getPTSByAddressId( $aid, $where, $count + 1, $uid );
    //$pts = $RO->getPTSAtHeight( 2214328 );

    //$wtxs = $api->get_transactions_where( $aid, $where, $uid, $count + 1 );

    $maxlen1 = 0;
    $maxlen2 = 0;
    $outs = [];

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
            ];
            $call = W8IO_ROOT . 'api/' . $wk->base58Encode( $wk->encryptash( json_encode( $call ) ) );
            $lazy = '</pre><pre class="lazyload" url="' . $call . '">...';
            if( $js )
                $REST->setTxsPagination( $call );
            break;
        }

        $type = $ts[TYPE];
        $asset = $ts[ASSET];
        $amount = $ts[AMOUNT];
        $a = $ts[A];
        $b = $ts[B];
        unset( $reclen );
        if( $type === TX_SPONSORSHIP && $aid !== false && $a !== $aid )
            continue;
        if( $aid !== false && $b === SELF )
        {
            $b = $a;
            $isa = true;
            $isb = true;
            
            if( $asset === WAVES_ASSET )
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
                if( $spam && !$isa && $info[1] === chr( 1 ) )
                    continue;

                $name = substr( $info, 2 );
                $amount = w8io_amount( $amount, $info[0], 0, false );
                $amount = ' ' . w8io_sign( $sign ) . $amount;
                $assetname = $name;
                $assetId = $asset;
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . $asset . '">' . $name . '</a>';
                $reclen = strlen( $amount ) + mb_strlen( html_entity_decode( $name ), 'UTF-8' );
            }
            else
            {
                $sign = ( ( $type === TX_LEASE_CANCEL ) ? -1 : 1 ) * ( ( $amount < 0 ) ? -1 : 1 ) * ( $isb ? ( $isa ? 0 : 1 ) : -1 );

                $amount = ' ' . w8io_sign( $sign ) . w8io_amount( $amount, 8, 0, false );
                $asset = ' <a href="' . W8IO_ROOT . $address . '/f/Waves">Waves</a>';
                $assetname = 'Waves';
                $assetId = WAVES_ASSET;
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
                $feename = substr( $info, 2 );
                $feeamount = w8io_amount( $fee, $info[0], 0 );
                $fee = ' <small>' . w8io_amount( $fee, $info[0], 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/' . $afee . '">' . substr( $info, 2 ) . '</a></small>';
            }
            else
            {
                $feename = 'Waves';
                $feeamount = w8io_amount( $fee, 8, 0 );
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
            if( $type === TX_TRANSFER ||
                $type === TX_LEASE ||
                $type === TX_ALIAS ||
                $type === TX_MASS_TRANSFER ||
                $type === TX_INVOKE )
            {
                $b = $RO->getAliasById( $addon );
                $addon = '';
                $isb = false;
            }
            else if( $type === TX_EXCHANGE )
            {
                $groupId = $ts[GROUP];
                $group = $RO->getGroupById( $groupId );
                if( $group === false )
                    w8_err( "getGroupById( $groupId )" );
                $pair = explode( '/', substr( $group, 1 ) );
                $buy = $RO->getAssetInfoById( (int)$pair[0] );
                $sell = $RO->getAssetInfoById( (int)$pair[1] );

                $bdecimals = (int)$buy[0];
                $bname = substr( $buy, 2 );
                $sdecimals = (int)$sell[0];
                $sname = substr( $sell, 2 );

                $price = $addon;
                if( $bdecimals !== 8 )
                    $price = substr( $price, 0, -8 + $bdecimals );

                if( $sdecimals )
                {
                    if( strlen( $price ) <= $sdecimals )
                        $price = str_pad( $price, $sdecimals + 1, '0', STR_PAD_LEFT );
                    $price = substr_replace( $price, '.', -$sdecimals, 0 );
                }
                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = ' ' . $price . $link . $bname . '/' . $sname . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );
            }
            else
                $addon = '';
        }

        if( $type === TX_INVOKE || $ts[FEEASSET] === INVOKE_ASSET )
        {
            $groupId = $ts[GROUP];
            if( $groupId > 0 )
            {
                $group = $RO->getGroupById( $groupId );
                if( $group === false )
                    w8_err( "getGroupById( $groupId )" );
                $pair = explode( '/', substr( $group, 1 ) );
                $addon = $RO->getFunctionById( (int)$pair[1] ) . '()';

                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . $addon . '</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );
            }
            else if( $groupId === -1 )
            {
                $link = ' <a href="' . W8IO_ROOT . 'txs/g/' . $groupId . '">';
                $linklen = strlen( $link ) + 3;
                $addon = $link . ':failed:</a>';
                $maxlen2 = max( $maxlen2, strlen( $addon ) - $linklen );
            }
        }

        $wtype = ( $ts[FEEASSET] === INVOKE_ASSET ? 'invoke ' : '' ) . w8io_tx_type( $type );
        $reclen += strlen( $wtype );
        $maxlen1 = max( $maxlen1, $reclen );
        $block = w8k2h( $ts[TXKEY] );

        if( $aid )
        {
            $act = $isa ? '&#183; ' : '&nbsp; ';
            $tar = $isa ? ( $isb ? '<>' : w8io_a( $b ) ) : w8io_a( $a );

            if( $js )
            {
                // $height, $time, $out, $type, $amount, $asset, $assetId, $address, $fee, $feeasset, $feeassetId
                $height = w8k2h( $ts[TXKEY] );
                $time = $RO->getTimestampByHeight( $height );
                $out = $isa ? 1 : 0;
                $type = $type;
                $amount = trim( $amount );
                $asset = $assetname;
                $assetId = $ts[ASSET];
                $address = $isa ? ( $isb ? '<>' : $b ) : $a;
                if( $isa && $fee )
                {
                    $fee = $feeamount;
                    $feeasset = $feename;
                    $feeassetId = $ts[FEEASSET];
                }
                else
                {
                    $fee = null;
                    $feeasset = null;
                    $feeassetId = null;
                }                

                $REST->setTxs( $height, $time, $out, $type, $amount, $asset, $assetId, $address, $fee, $feeasset, $feeassetId );
            }
            else
            {
                $date = date( 'Y.m.d H:i', $RO->getTimestampByHeight( w8k2h( $ts[TXKEY] ) ) );
                $txkey = '<a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . $date . '</a>';

                $outs[] = [
                    $act,
                    ( $isa ? '<b>' : '' ) . '<small>' . $act . $txkey . '&nbsp;<a href="' . W8IO_ROOT . 'b/' . $block . '">&#183;</a>' .
                    ' </small><a href="' . W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a>' . $amount . $asset . ( $isa ? '</b>' : '' ),
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
            echo
                '<small><a href="' . W8IO_ROOT . 'tx/' . $ts[TXKEY] . '">' . date( 'Y.m.d H:i', $RO->getTimestampByHeight( w8k2h( $ts[TXKEY] ) ) ) . '</a>' .
                ' <a href="' . W8IO_ROOT . 'b/' . $block . '">[' . $block . ']</a></small>' .
                ' <a href="' . W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a> ' . w8io_a( $a ) . ' > ' . w8io_a( $b ) .
                $addon . $amount . $asset . PHP_EOL;
        }
    }

    if( $js )
        return;

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
            $pad2 = $p2pad > 3 ? ( ' ' . str_repeat( '-', $p2pad - 1 ) ) : str_repeat( ' ', $p2pad );
        }
        $pad1 = $p1pad > 3 ? ( ' ' . str_repeat( '-', $p1pad - 1 ) ) : str_repeat( ' ', $p1pad );

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

function w8io_height( $height )
{
    return '<a href=' . W8IO_ROOT . 'b/' . $height . '>' . $height . '</a>';
}

function w8io_txid( $txid )
{
    return '<a href=' . W8IO_ROOT . 'tx/' . $txid . '>' . $txid . '</a>';
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
                    case 'id': $v = w8io_txid( $v ); break;
                    case 'sender': 
                    case 'recipient':
                    case 'target': $v = w8io_a( $v ); break;
                    case 'attachment': $fkv[$k . '-decoded'] = htmlentities( trim( preg_replace( '/\s+/', ' ', wk()->base58Decode( $v ) ) ) );
                    default: $v = htmlentities( $v );
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
        $result = 'Previous script: ' . w8io_txid( $tx['id'] ) . PHP_EOL . PHP_EOL;

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
        $result .= '<style>' . file_get_contents( 'vendor/jfcherng/php-diff/example/diff-table.css' ) . '</style>';
        $result .= 'Diff: ' . PHP_EOL . \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'SideBySide', [], [ 'detailLevel' => 'word' ] ) . PHP_EOL;
        if( !empty( $decompile2 ) )
            $result .= 'Full: ' . PHP_EOL . \Jfcherng\Diff\DiffHelper::calculate( $decompile2, $decompile1, 'SideBySide', [ 'context' => \Jfcherng\Diff\Differ::CONTEXT_ALL ], [ 'detailLevel' => 'word' ] );
    }
    
    return $result;
}

if( $address === 'CHARTS' )
{/*
    $height = $api->get_height();

    $from = $f ? (int)$f : 0;
    $to = $arg ? (int)$arg : $height;

    if( $from > $to )
        exit;

    $to = min( $height, $to );
    $Q = 1;

    while( ( $to - $from ) / $Q > 2500 && $Q < 1000 )
        $Q *= 10;

    $from -= $from % $Q;
    $froms = max( 1, $from );
    $title = "Waves " . ( W8IO_NETWORK == 'W' ? "MAINNET" : "TESTNET" );

    require_once './include/w8io_charts.php';
    $s = isset( $_SERVER['HTTPS'] ) && $_SERVER['HTTPS'] === 'on';
    $s |= isset( $_SERVER['HTTP_X_FORWARDED_PROTO'] ) && $_SERVER['HTTP_X_FORWARDED_PROTO'] === 'https';
    $hostroot = 'http' . ( $s ? 's' : '' ) . '://' . $_SERVER['HTTP_HOST'] . W8IO_ROOT;
    echo w8io_chart( $title, "$froms .. $to", $hostroot . "api/chart/$from/$to" );
*/}
else
if( $address === 'tx' && isset( $f ) )
{
    $l = strlen( $f );
    if( $l > 40 )
    {
        $wk = wk();
        $tx = $wk->getTransactionById( $f );
        if( $tx === false )
            echo json_encode( [ 'error' => "getTransactionById( $f ) failed" ], JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
        else
        {
            if( !empty( $tx['script'] ) )
                $addon = htmlscript( $tx );
            $tx = htmlfilter( $tx );
            echo json_encode( $tx, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
            if( isset( $addon ) )
                echo PHP_EOL . PHP_EOL . $addon;
        } 
    }
}
else
if( $address === 'CAP' && isset( $f ) )
{
    require_once 'include/RO.php';
    $RO = new RO( W8DB );

    $group = (int)$f;
    $height = $RO->getLastHeightTimestamp()[0] - 1500;
    $gs = explode( '/', substr( $RO->getGroupById( $group ), 1 ) );

    echo '<table><tr>';
    for( $i = 0; $i < 1; $i++ )
    {
        $adds = [];
        $addsv = [];
        $addsp = [];
        $exchanges = $RO->db->query( 'SELECT * FROM pts WHERE r1 > ' . w8h2k( $height ) );
        $n = 0;
        $txs = [];
        foreach( $exchanges as $ts )
        {
            $type = (int)$ts[TYPE];
            if( $type !== ( !$i ? 7 : 4 ) )
                continue;

            $a = (int)$ts[A];
            if( $a <= 0 )
                continue;

            if( $group !== (int)$ts[GROUP] )
                continue;

            $txs[(int)$ts[UID]] = $ts;
        }

        krsort( $txs );

        $bots = [];

        $uid = [];
        foreach( $txs as $ts )
        {
            $a = (int)$ts[A];
            $b = (int)$ts[B];
            $amount = (int)$ts[AMOUNT];
            $price = (int)$ts[ADDON] / 100000000;
            
            if( !isset( $uid[$a] ) )
                $uid[$a] = (int)$ts[UID];
            if( !isset( $uid[$b] ) )
                $uid[$b] = (int)$ts[UID];
            if( !isset( $adds[$a] ) )
                $adds[$a] = 0;
            if( !isset( $adds[$b] ) )
                $adds[$b] = 0;
            if( !isset( $addsv[$a] ) )
                $addsv[$a] = 0;
            if( !isset( $addsv[$b] ) )
                $addsv[$b] = 0;
            if( !isset( $addsp[$a] ) )
                $addsp[$a] = [ 10000000000, 0, 0, $price ];
            if( !isset( $addsp[$b] ) )
                $addsp[$b] = [ 10000000000, 0, 0, $price ];

            $adds[$a] -= $amount;
            $adds[$b] += $amount;

            $addsv[$a] += $amount;
            $addsv[$b] += $amount;

            $addsp[$a][0] = min( $addsp[$a][0], $price );
            $addsp[$b][0] = min( $addsp[$b][0], $price );
            $addsp[$a][1] = max( $addsp[$a][1], $price );
            $addsp[$b][1] = max( $addsp[$b][1], $price );
            $addsp[$a][2]++;
            $addsp[$b][2]++;
            //$addsp[$a][3] = ( $addsp[$a][3] * ( $addsp[$a][2] - 1 ) + $price ) / $addsp[$a][2];
            //$addsp[$b][3] = ( $addsp[$b][3] * ( $addsp[$b][2] - 1 ) + $price ) / $addsp[$b][2];

            //if( (int)$tx['timestamp'] >= 1575275064 && (int)$tx['timestamp'] <= 1575290274 )
            {
                //$bots[$api->get_address( $a )] = true;
                //$bots[$api->get_address( $b )] = true;
            }
        }

        //$bots = array_keys( $bots );
        //file_put_contents( 'bots.txt', json_encode( $bots, JSON_PRETTY_PRINT ) );
        $bots = wk()->json_decode( file_get_contents( 'bots.txt' ) );

        //$adds = array_reverse( $adds, true );
        arsort( $uid );

        echo '<td valign=top>';
        foreach( $uid as $address => $t )
        {
            $amount = $adds[$address];
            $address = $RO->getAddressById( $address );
            if( in_array( $address, $bots ) )
                echo '<b>';
            echo w8io_a( $address ) . ' = ' . w8io_amount( $amount, 8, 0 ) . '&nbsp;<br>';
            if( in_array( $address, $bots ) )
                echo '</b>';
        }
        echo '</td>';

        echo '<td valign=top>';
        foreach( $uid as $address => $t )
        {
            $volume = $addsv[$address];
            if( $volume === $adds[$address] )
                echo '<b>';
            echo '('. w8io_amount( $volume, 8, 0 ) . ') ('.$addsp[$address][2].')&nbsp;<br>';
            if( $volume === $adds[$address] )
                echo '</b>';
        }
        echo '</td>';

        echo '<td valign=top>';
        foreach( $uid as $address => $t )
        {
            if( $addsp[$address][3] < 0.0001 )
            {
                echo '&nbsp;<br>';
                continue;
            }
            $price_min = sprintf( '%0.4f', $addsp[$address][0] );
            $price_max = sprintf( '%0.4f', $addsp[$address][1] );
            $price_med = sprintf( '%0.4f', $addsp[$address][3] );
            echo '['. $price_min. ', '.$price_med.', '.$price_max.']&nbsp;<br>';
        }
        echo '</td>';

        echo '<td valign=top>';
        foreach( $uid as $address => $t )
        {
            $balance = $RO->getBalanceByAddressId( $address );
            echo str_pad( '(' . w8io_amount( $balance[(int)$gs[0]], 8, 0 ) . ')', 20 ) . '(' . w8io_amount( $balance[(int)$gs[1]], 8, 0 ) . ')&nbsp;&nbsp;&nbsp;<br>';
        }
        echo '</td>';

        arsort( $addsv );

        echo '<td valign=top>';
        foreach( $addsv as $address => $amount )
        {
            $volume = $adds[$address];
            $address = $RO->getAddressById( $address );
            echo w8io_a( $address ) . ' = ' . w8io_amount( $amount, 8, 0 ) . ' ('. w8io_amount( $volume, 8, 0 ) .')&nbsp;<br>';
        }
        echo '</td>';

        echo '<td valign=top>';
        foreach( $addsv as $address => $amount )
        {
            $price_min = sprintf( '%0.4f', $addsp[$address][0] );
            $price_max = sprintf( '%0.4f', $addsp[$address][1] );
            $price_med = sprintf( '%0.4f', $addsp[$address][3] );
            echo '['. $price_min. ', '.$price_med.', '.$price_max.'] &nbsp;<br>';
        }
        echo '</td>';
    }
    echo '</tr></table>';
}
else
if( $address === 'b' )
{
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
if( $address === 'GENERATORS' )
{
    $arg = isset( $showtime ) && $arg !== false ? intval( $arg ) : null;

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
        if( isset( $arg ) )
            $balance = $api->correct_balance( $generator, $arg, $arg > WAVES_LEASE_ASSET ? $balance : null );
        $gentotal += $balance;

        foreach( $pts as $height => $ts )
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
    $qb = max( intdiv( $q, 16 ), 5 );

    $period = $totime - $fromtime;
    $period = round( $period / 3600 );
    if( $period < 100 )
        $period = $period . ' h';
    else
        $period = round( $period / 24 ) . ' d';

    $totime = date( 'Y.m.d H:i', $totime );

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
    uasort( $generators, function( $a, $b ){ return( $a['balance'] < $b['balance'] ); } );

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
        foreach( $pts as $ts )
        {
            $block = w8k2h( (int)$ts[TXKEY] );
            $target = $Q - 1 - (int)floor( ( $to - $block ) / $q );
            $matrix[$target]++;
            $fee += (int)$ts[AMOUNT];
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
{
    if( !isset( $RO ) )
    {
        require_once 'include/RO.php';
        $RO = new RO( W8DB );
    }

    if( $js )
    {
        require_once 'include/REST.php';
        $REST = new REST( $RO );
    }

    $aid = $RO->getAddressIdByString( $address );

    $where = false;
    if( $f === 'f' )
        $where = "r5 = $arg";
    else if( $f === 't' )
        $where = "r2 = $arg";
    else if( $aid === false && $f === 'g' )
        $where = "r10 = $arg";

    if( $aid === false )
    {
        //$assetId = $address === 'WAVES' ? 0 : $RO->getAssetInfoById( $address );
        //if( $assetId === false )
        {
            echo '<pre>';
            w8io_print_transactions( false, $where, false, 100, $address, !( $f === 'f' ) );
            echo '</pre>';
        }
        //else
        //{
        //    echo '<pre>';
        //    w8io_print_distribution( $assetId );
        //    echo '</pre>';
        //}
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

        $heightTime = $RO->getLastHeightTimestamp();
        $time = date( 'Y.m.d H:i', $heightTime[1] );
        $height = $heightTime[0];
        
        if( $js )
            $REST->setHeader( $height, $heightTime[1], $address, $full_address );
        else
        {
            $full_address = $full_address !== $address ? " / <a href=\"". W8IO_ROOT . $full_address ."\">$full_address</a>" : '';
            echo "<a href=\"". W8IO_ROOT . $address ."\">$address</a>$full_address @ $height <small>($time)</small>" . PHP_EOL . PHP_EOL;
            echo '<table><tr><td valign="top"><pre>';
        }

        $tickers = [];
        $unlisted = [];

        if( !isset( $balance[0] ) )
            $balance[0] = 0;

        $weights = [];
        $prints = [];

        // WAVES
        {
            $asset = "Waves";
            $amount = w8io_amount( $balance[0], 8 );
            $furl = W8IO_ROOT . $address . '/f/Waves';

            if( $arg === 0 && $f !== 't' )
            {
                echo '<b>' . $amount . ' <a href="' . $furl . '">' . $asset . '</a></b>' . PHP_EOL;
                echo '<span style="color:#606870">' . str_repeat( '—', 38 ) . '&nbsp;</span>' .  PHP_EOL;
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
            if( $amount === 0 )
                continue;

            if( $asset > 0 )
            {
                $info = $RO->getAssetInfoById( $asset );
                if( $info[1] === chr( 1 ) && $arg !== $asset )
                    continue;

                $id = $asset;
                $b = $asset === $arg;
                $decimals = (int)$info[0];
                $asset = substr( $info, 2 );
                $amount = w8io_amount( $amount, $decimals );

                $furl = W8IO_ROOT . $address . '/f/' . $id;

                $record = [ 'id' => $id, 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];

                if( $b && !$js )
                    $frecord = $record;
                else
                {
                    $weights[$id] = ord( $info[1] );
                    $prints[$id] = $record;
                }
            }
        }

        if( isset( $frecord ) )
        {
            echo '<b>' . $frecord['amount'] . ' <a href="' . $frecord['furl'] . '">' . $frecord['asset'] . '</a></b>' . PHP_EOL;
            echo '<span style="color:#606870">' . str_repeat( '—', 38 ) . '&nbsp;</span>' .  PHP_EOL;
        }

        arsort( $weights );

        foreach( $weights as $asset => $weight )
        {
            if( $weight === 0 && !isset( $zerotrades ) )
            {
                if( !$js )
                    echo '<span style="color:#606870">' . str_repeat( '—', 38 ) . '&nbsp;</span>' .  PHP_EOL;
                $zerotrades = true;
            }

            $record = $prints[$asset];
            if( $js )
                $REST->setBalance( $asset, $weight, trim( $record['amount'] ), $record['asset'] );
            else
                echo $record['amount'] . ' <a href="' . $record['furl'] . '">' . $record['asset'] . '</a>' . PHP_EOL;
        }

        if( !isset( $zerotrades ) && !$js )
        {
            echo '<span style="color:#606870">' . str_repeat( '—', 38 ) . '&nbsp;</span>' .  PHP_EOL;
            $zerotrades = true;
        }

        if( !$js )
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
            w8io_print_transactions( $aid, $where, false, 100, $address, !( $f === 'f' ) );
    }
}

if( $js )
{
    header( 'Access-Control-Allow-Origin: *' );
    header( 'Access-Control-Allow-Methods: GET' );
    header( 'Access-Control-Allow-Headers: X-Requested-With' );
    exit( json_encode( $REST->j, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES ) );
}

echo '</pre></td></tr></table>';
echo '<hr><div width="100%" align="right"><pre><small>';
echo "<a href=\"https://github.com/deemru/w8io\">github/deemru/w8io</a>";
if( file_exists( '.git/FETCH_HEAD' ) )
{
    $rev = file_get_contents( '.git/FETCH_HEAD', null, null, 0, 40 );
    echo "/<a href=\"https://github.com/deemru/w8io/commit/$rev\">" . substr( $rev, 0, 7 ) . '</a> ';
}
if( !isset( $showtime ) )
{
    echo PHP_EOL . sprintf( '%.02f ms ', 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] ) );
    $light = $light ? '&#9680' : '&#9681;';
    echo '<a href="'. W8IO_ROOT . "ld\" style=\"text-decoration: none;\">$light</a> ";
    if( defined( 'W8IO_ANALYTICS' ) )
        echo PHP_EOL . PHP_EOL . W8IO_ANALYTICS . ' ';
}
echo '</small></pre></div>';
echo '
        </pre>
    </body>
</html>';

if( isset( $showtime ) )
{
    file_put_contents( $showfile, ob_get_contents() );
    ob_end_clean();
    exit( file_get_contents( $showfile ) );
}
