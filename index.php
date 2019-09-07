<?php

require_once __DIR__ . '/vendor/autoload.php';
require_once 'w8io_config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = 'blocks/1004021';
    //$uri = 't';

$uri = explode( '/', preg_filter( '/[^a-zA-Z0-9_.@\-\/]+/', '', $uri . chr( 0 ) ) );

$address = $uri[0];
$f = isset( $uri[1] ) ? $uri[1] : false;
$arg = isset( $uri[2] ) ? $uri[2] : false;
$arg2 = isset( $uri[3] ) ? $uri[3] : false;
$arg3 = isset( $uri[4] ) ? $uri[4] : false;
$arg4 = isset( $uri[5] ) ? $uri[5] : false;

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
    require_once './include/w8io_blockchain_transactions.php';
    require_once './include/w8io_api.php';
    $api = new w8io_api();

    $txid = $api->get_transactions_id( $f );
    if( $txid !== false )
    {
        if( strlen( $txid ) >= 32 )
            exit( header( 'location: ' . W8IO_ROOT . 'tx/' . base58Encode( $txid ) ) );
        else
            exit( header( 'location: ' . W8IO_ROOT . 'blocks/' . $txid ) );
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
                require_once './include/w8io_blockchain_transactions.php';
                require_once './include/w8io_api.php';
                $api = new w8io_api();
                
                $aid = $call['i'];
                $where = $call['w'];
                $uid = $call['u'];
                $address = $call['a'];

                w8io_print_transactions( $aid, $where, $uid, 100, $address, false === strpos( $where, 'asset' ) );
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

        while( ( $to - $from ) / $Q > 2500 && $Q < 1000 )
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

if( $light )
{
    $bcolor = 'FFFFFF';
    $tcolor = '000000';
}
else
{
    $bcolor = '404840';
    $tcolor = 'A0A8C0';
}

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
        <link rel="shortcut icon" href="%sfavicon.ico" type="image/x-icon">
        <script type="text/javascript" src="%sjquery.js" charset="UTF-8"></script>
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
            g_lazyload.remove();

            if( data )
            {
                $(".base").append( data );

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
        font-size: 12pt; font-size: %s; font-family: monospace, monospace;
        background-color: #%s;
        color: #%s;
        border-collapse: collapse;
        overflow-y: scroll;%s
    }
    a
    {
        color: #%s;%s
        text-decoration: none;
        border-bottom: 1px dotted #606870;
        
    }
    a:hover
    {
        border-bottom: 2px solid #%s;
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
', empty( $address ) ? '' : " / $address", W8IO_ROOT, W8IO_ROOT,
isset( $showtime ) ? '0.66vw' : '0.90vw',
$bcolor, $tcolor,
isset( $showtime ) ? 'margin: 1em 2em 1em 2em; filter: brightness(144%);' : '',
$tcolor,
isset( $showtime ) ? 'text-decoration: none;' : '',
$tcolor );

if( empty( $address ) )
    $address = 'GENESIS';

require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';
require_once './include/w8io_api.php';

$api = new w8io_api();

if( $f === 'f' )
{
    if( $arg === 'Waves' )
        $arg = 0;
    else
    {
        $arg = $api->get_asset( $arg );
        if( $arg === false )
            w8io_error( 'unknown asset' );
    }
}
elseif( $arg !== false )
    $arg = (int)$arg;

function w8io_amount( $amount, $decimals, $pad = 20 )
{
    if( $amount < 0 )
    {
        $sign = '-';
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

function w8io_print_transactions( $aid, $where, $uid, $count, $address, $spam = true )
{
    global $api;

    $wtxs = $api->get_transactions_where( $aid, $where, $uid, $count + 1 );

    $n = 0;
    foreach( $wtxs as $wtx )
    {
        if( $count && ++$n > $count )
        {
            $wk = wk();
            $call = [
                'f' => 't',
                'i' => $aid,
                'w' => $where,
                'u' => $wtx['uid'],
                'a' => $address,
            ];
            $call = W8IO_ROOT . 'api/' . $wk->base58Encode( $wk->encryptash( json_encode( $call ) ) );
            echo '<pre class="lazyload" url="' . $call . '">...</pre>';
            return;
        }

        $wtx = w8io_filter_wtx( $wtx );

        $type = $wtx['type'];
        $asset = $wtx['asset'];
        $amount = $wtx['amount'];
        $a = $wtx['a'];
        $b = $wtx['b'];

        if( $asset )
        {
            $info = $api->get_asset_info( $asset );
            if( $spam && isset( $info['scam'] ) )
                continue;

            $amount = ' ' . ( $b === $aid ? '+' : '-' ) . w8io_amount( $amount, $info['decimals'], 0 );
            $asset = ' <a href="' . W8IO_ROOT . $address . '/f/' . $info['id'] . '">' . $info['name'] . '</a>';
        }
        else if( $b !== - 3 )
        {
            $amount = ' ' . ( ( $type === 9 ^ $b === $aid ) ? '+' : '-' ) . w8io_amount( $amount, 8, 0 );
            $asset = ' <a href="' . W8IO_ROOT . $address . '/f/Waves">Waves</a>';
        }
        else
        {
            $asset = '';
            $amount = '';
        }

        $isa = $a === $aid;
        $isb = $b === $aid;
        $a = $isa ? $address : $api->get_address( $a );
        $b = $isb ? $address : $api->get_address( $b );

        $fee = $wtx['fee'];

        if( $a === $address && $fee )
        {
            $afee = $wtx['afee'];

            if( $afee )
            {
                $info = $api->get_asset_info( $afee );
                $fee = ' <small>(' . w8io_amount( $fee, $info['decimals'], 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/' . $info['id'] . '">' . $info['name'] . '</a> fee)</small>';
            }
            else
            {
                $fee = ' <small>(' . w8io_amount( $fee, 8, 0 ) . ' <a href="' . W8IO_ROOT . $address . '/f/Waves">Waves</a> fee)</small>';
            }
        }
        else
            $fee = '';

        $data = $wtx['data'];

        if( $data )
        {
            $data = json_decode( $data, true );

            if( $type === 10 )
                $b = $api->get_data( $data['d'] );
            else
            if( isset( $data['b'] ) )
                $b = $api->get_data( $data['b'] );
        }

        $wtype = w8io_tx_type( $type );

        $ashow = $isa ? "<b>$a</b>" : $a;
        $bshow = $isb ? "<b>$b</b>" : $b;

        $block = $wtx['block'];

        echo
            '<small><a href="' . W8IO_ROOT . 'tx/' . $wtx['txid'] . '">' .
            date( 'Y.m.d H:i', $wtx['timestamp'] ) . '</a> (<a href="' . W8IO_ROOT . 'blocks/' . $block . '">' . $block . '</a>)</small> ' .
            '(<a href="' . W8IO_ROOT . $address . '/t/' . $type . '">' . $wtype . '</a>) ' .
            '<a href="' . W8IO_ROOT . $a . '">' . ( $isa ? '<b>' . $a . '</b>' : $a ) . '</a> -> ' .
            '<a href="' . W8IO_ROOT . $b . '">' . ( $isb ? '<b>' . $b . '</b>' : $b ) . '</a>' .
            $amount . $asset . $fee . PHP_EOL;
    }
}

function prios( $tickers )
{
    $t_prios = [];
    $t_other = [];

    $prios = [
        '8LQW8f7P5d5PZM7GtZEBgaqRPGSzS3DfPuiXrURJ4AJS', // WBTC
        '474jTeYx2r2Va35794tCScAXWJG9hU2HcgxzMowaZUnu', // WETH
        'Ft8X1v1LTa1ABafufpaCWyVj8KkaxUWE6xBhW6sNFJck', // WUSD
        'Gtb1WRznfchDnTh37ezoDTJ4wcoKaRsKqKjJjy7nm2zU', // WEUR
        '2mX5DzVKWrAJw8iwdJnV2qtoeVG9h5nTDpTqC1wb1WEN', // WTRY
        'HZk1mbfuJpmxU1Fs4AX5MWLVYtctsNcg6e2C6VKqK8zk', // Litecoin
        'zMFqXuoyrn5w17PFurTqxB7GsS71fp9dfk6XFwxbPCy',  // Bitcoin Cash
        'BrjUWjndUanm5VsJkbUip8VRYy6LWJePtxya3FNv4TQa', // Zcash
        'B3uGHFRpSUuGEDWjqB9LWWxafQj8VTvpMucEyoxzws5H', // DASH
        '7FzrHF1pueRFrPEupz6oiVGTUZqe8epvC7ggWUx8n1bd', // Liquid
        'DHgwrRvVyqJsepd32YbBqUeDH4GJ1N984X8QoekjgH8J', // WavesCommunity
        '4uK8i4ThRGbehENwa6MxyLtxAjAo1Rj9fduborGExarC', // MinersReward
    ];

    foreach( $tickers as $record )
        if( !isset( $record['id'] ) || in_array( $record['id'], $prios, true ) )
            $t_prios[] = $record;
        else
            $t_other[] = $record;

    return array_merge( $t_prios, $t_other );
}

function w8io_a( $address )
{
    if( $address[0] === 'a' )
        $address = substr( $address, 8 );
    return '<a href=' . W8IO_ROOT . $address . '>' . $address . '</a>';
}

function w8io_txid( $txid )
{
    return '<a href=' . W8IO_ROOT . 'tx/' . $txid . '>' . $txid . '</a>';
}

function txproc( &$tx )
{
    $tx['id'] = w8io_txid( $tx['id'] );
    $tx['sender'] = w8io_a( $tx['sender'] );
    if( isset( $tx['target'] ) )
        $tx['target'] = w8io_a( $tx['target'] );
    if( isset( $tx['recipient'] ) )
        $tx['recipient'] = w8io_a( $tx['recipient'] );
    
    switch( $tx['type'] )
    {
        case 7:
        {
            $tx['order1']['sender'] = w8io_a( $tx['order1']['sender'] );
            $tx['order2']['sender'] = w8io_a( $tx['order2']['sender'] );
            break;
        }
        default:
            break;
    }
}

if( $address === 'CHARTS' )
{
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
    $hostroot = 'http' . ( ( isset( $_SERVER['HTTPS'] ) && $_SERVER['HTTPS'] == 'on' ) ? 's' : '' ) . '://' . $_SERVER['HTTP_HOST'] . W8IO_ROOT;
    echo w8io_chart( $title, "$froms .. $to", $hostroot . "api/chart/$from/$to" );
}
else
if( $address === 'tx' && isset( $f ) )
{
    $l = strlen( $f );
    if( $l > 40 )
    {
        $wk = wk();
        $tx = $wk->getTransactionById( $f );
        txproc( $tx );
        echo json_encode( $tx, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
    }    
}
else
if( $address === 'blocks' )
{
    $height = (int)$f;
    $block = $api->getBlockAt( (int)$f );
    $txs = $block['transactions'];
    unset( $block['transactions'] );

    if( $block === false )
    {
        echo 'block not found';
    }
    else
    {
        $block['generator'] = w8io_a( $block['generator'] );
        if( $height > 1 )
        {
            unset( $block['height'] );
            $block['previous'] = $height - 1;
        }
        $block['height'] = $height;
        $block['next'] = $height + 1;
        foreach( $txs as &$tx )
            txproc( $tx );
        $block['transactions'] = $txs;
        echo json_encode( $block, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES );
    }
}
else
if( $address === 'GENERATORS' )
{
    $arg = isset( $showtime ) && $arg !== false ? intval( $arg ) : null;
    $generators = $api->get_generators( $n, $arg );

    $Q = isset( $showtime ) ? 128 : 80;
    $infos = [];
    $gentotal = 0;
    $feetotal = 0;
    $blktotal = 0;

    foreach( $generators as $generator => $wtxs )
    {
        $balance = $api->get_address_balance( $generator );
        $balance = ( isset( $balance['balance'][0] ) ? $balance['balance'][0] : 0 ) + ( isset( $balance['balance'][W8IO_ASSET_WAVES_LEASED] ) ? $balance['balance'][W8IO_ASSET_WAVES_LEASED] : 0 );
        if( isset( $arg ) )
            $balance = $api->correct_balance( $generator, $arg, $arg > W8IO_RESET_LEASES ? $balance : null );
        $gentotal += $balance;

        foreach( $wtxs as $wtx )
        {
            $block = $wtx['block'];
            if( !isset( $from ) || $from > $block )
            {
                $from = $block;
                $fromtime = $wtx['timestamp'];
            }
            if( !isset( $to ) || $to < $block )
            {
                $to = $block;
                $totime = $wtx['timestamp'];
            }
        }
        
        $infos[$generator] = array( 'balance' => $balance, 'wtxs' => $wtxs );
    }

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
            $highlights[] = $Q - 1 - (int)floor( ( $to - 1 ) / $q );
        while( $he !== $hs )
            $highlights[] = $Q - 1 - (int)floor( ( $to - ( ++$hs * 10000 ) ) / $q );

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
        $address = $api->get_address( $id );
        $alias = $api->get_alias_by_id( $id );
        $padlen = max( 30 - strlen( $alias ), 0 );

        $address = '<a href="' . W8IO_ROOT . "$address\">$address</a>";
        $alias = $alias === false ? '  ' : '(<a href="' . W8IO_ROOT . "$alias\">$alias</a>)";
        $alias .= str_pad( '', $padlen );

        $balance = $generator['balance'];
        $percent = str_pad( number_format( $gentotal ? ( $balance / $gentotal * 100 ) : 0, 2, '.', '' ) . '%', 7, ' ', STR_PAD_LEFT );
        $balance = str_pad( number_format( $balance / 100000000, 0, '', "'" ), 10, ' ', STR_PAD_LEFT );

        $wtxs = $generator['wtxs'];
        $count = count( $wtxs );
        $blktotal += $count;

        $matrix = array_fill( 0, $Q, 0 );
        $fee = 0;
        foreach( $wtxs as $wtx )
        {
            $block = $wtx['block'];
            $target = $Q - 1 - (int)floor( ( $to - $block ) / $q );
            $matrix[$target]++;
            $fee += $wtx['amount'];
        }

        $feetotal += $fee;
        $fee = w8io_amount( $fee, 8, 14 );

        $mxprint = '';
        for( $i = 0; $i < $Q; $i++ )
        {
            $blocks = $matrix[$i];
            if( $blocks === 0 )
                $blocks = '.';
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
    $gentotal = str_pad( number_format( $gentotal / 100000000, 0, '', "'" ), 80, ' ', STR_PAD_LEFT );
    $feetotal = str_pad( number_format( $feetotal / 100000000, 8, '.', '' ), ( isset( $showtime ) ? 48 : 0 ) + 104, ' ', STR_PAD_LEFT );

    echo "<small style=\"font-size: 50%;\"><br></small><b>$ntotal $gentotal $feetotal</b> ($blktotal)" .  PHP_EOL;

    if( isset( $showtime ) )
        for( $i = $n; $i <= 64; $i++ )
            echo PHP_EOL;
}
else
{
    $where = false;
    if( $f === 'f' )
        $where = "asset = $arg";
    else if( $f === 't' )
        $where = "type = $arg";
    else if( $f === 't-' )
        $where = "type != $arg";

    $aid = $api->get_aid( $address );
    if( $aid === false )
    {
        $assetId = $address === 'WAVES' ? 0 : $api->get_asset( $address );
        if( $assetId === false )
        {
            echo '<pre class="base">';
            w8io_print_transactions( false, $where, false, 100, $address, !( $f === 'f' ) );
            echo '</pre>';
        }
        else
        {
            echo '<pre class="base">';
            w8io_print_distribution( $assetId );
            echo '</pre>';
        }
    }
    else
    {
        $full_address = $api->get_address( $aid );
        $balance = $api->get_address_balance( $aid );

        if( $balance === false )
            w8io_error( "get_address_balance( $aid ) failed" );

        $height = $balance['height'];
        $balance = $balance['balance'];
        $full_address = $full_address !== $address ? " / <a href=\"". W8IO_ROOT . $full_address ."\">$full_address</a>" : '';

        echo "<a href=\"". W8IO_ROOT . $address ."\">$address</a>$full_address @ $height" . PHP_EOL;
        echo '<table><tr><td valign="top"><pre>';

        $tickers = [];
        $unlisted = [];

        if( !isset( $balance[0] ) )
            $balance[0] = 0;

        // WAVES
        {
            $asset = "Waves";
            $amount = w8io_amount( $balance[0], 8 );

            $furl = W8IO_ROOT . "$address/f/Waves";

            $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl, 'b' => $arg === 0 && $f !== 't' ];
        }

        if( isset( $balance[W8IO_ASSET_WAVES_LEASED] ) )
        {
            $amount = $balance[W8IO_ASSET_WAVES_LEASED] + ( isset( $balance[0] ) ? $balance[0] : 0 );

            if( $balance[0] !== $amount )
            {
                $asset = "Waves (GENERATOR)";
                $amount = w8io_amount( $amount, 8 );

                $furl = W8IO_ROOT . "$address/f/Waves";

                $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl, 'b' => false ];
            }
        }

        foreach( $balance as $asset => $amount )
        {
            if( $amount === 0 )
                continue;

            if( $asset > 0 )
            {
                $info = $api->get_asset_info( $asset );
                if( isset( $info['scam'] ) )
                    continue;

                $b = $asset === $arg;
                $asset = $info['name'];
                $decimals = $info['decimals'];
                $amount = w8io_amount( $amount, $decimals );

                $id = $info['id'];
                $furl = W8IO_ROOT . "$address/f/$id";

                $record = [ 'asset' => $asset, 'id' => $id, 'amount' => $amount, 'furl' => $furl, 'b' => $b ];

                if( isset( $info['ticker'] ) )
                    $tickers[] = $record;
                else
                    $unlisted[] = $record;
            }
        }

        $tickers = prios( $tickers );
        $unlisted = prios( $unlisted );

        foreach( $tickers as $record )
        {
            if( $record['b'] )
            {
                $bs = '<b>';
                $be = '</b>';
            }
            else
            {
                $bs = $be = '';
            }
            echo "$bs{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>$be" . PHP_EOL;
        }

        echo '<span style="color:#606870">'.str_repeat( '—', 38) . '&nbsp;</span>' .  PHP_EOL;

        foreach( $unlisted as $record )
        {
            if( $record['b'] )
            {
                $bs = '<b>';
                $be = '</b>';
            }
            else
            {
                $bs = $be = '';
            }
            echo "$bs{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>$be" . PHP_EOL;
        }

        echo '</pre></td><td valign="top"><pre class="base">';

        if( $f !== 'pay' )
            w8io_print_transactions( $aid, $where, false, 100, $address, !( $f === 'f' ) );
        else
        {
            $from = $arg;
            $to = $arg2;

            $incomes = $api->get_incomes( $aid, $from, $to );

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
                        $address = $api->get_address( $a );
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
                $query = $api->get_transactions_query( "SELECT * FROM transactions WHERE block >= $from AND block <= $to AND b = $aid AND type = 0" );
                foreach( $query as $wtx )
                {
                    $wtx = w8io_filter_wtx( $wtx );
                    if( $wtx['asset'] === 0 )
                    {
                        $waves_fees += $wtx['amount'];
                        $waves_blocks++;
                    }
                }
                $waves_fees = intval( $waves_fees * $percent / 100 );

                // mrt_fees
                $mrt_fees = 0;
                $mrt_id = $api->get_asset( '4uK8i4ThRGbehENwa6MxyLtxAjAo1Rj9fduborGExarC' );
                $query = $api->get_transactions_query( "SELECT * FROM transactions WHERE block >= $from AND block <= $to AND b = $aid AND type = 4 AND asset = $mrt_id" );
                foreach( $query as $wtx )
                {
                    $wtx = w8io_filter_wtx( $wtx );
                    $mrt_fees += $wtx['amount'];
                }
                $query = $api->get_transactions_query( "SELECT * FROM transactions WHERE block >= $from AND block <= $to AND b = $aid AND type = 11 AND asset = $mrt_id" );
                foreach( $query as $wtx )
                {
                    $wtx = w8io_filter_wtx( $wtx );
                    $mrt_fees += $wtx['amount'];
                }
                $mrt_fees = intval( $mrt_fees * $percent / 100 );

                echo "pay ($from .. $to) ($percent %):" . PHP_EOL . PHP_EOL;
                echo w8io_amount( $waves_blocks, 0 ) . ' Blocks' . PHP_EOL;
                echo w8io_amount( $waves_fees, 8 ) . " Waves" . PHP_EOL;
                echo w8io_amount( $mrt_fees, 2 ) . " MinersReward" . PHP_EOL;

                $payments = [];
                foreach( $incomes as $a => $p )
                    if( $p * $waves_fees > 10000 && ( $mrt_fees === 0 || $p * $mrt_fees > 1 ) )
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
                    $address = $api->get_address( $a );
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
                        echo "    Mass (MinersReward) #$m:" . PHP_EOL;
                        echo "    ------------------------------------------------------------" . PHP_EOL;
                    }
                    $address = $api->get_address( $a );
                    $pay = number_format( $p * $mrt_fees / 100, 2, '.', '' );
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
    }
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
