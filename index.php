<?php

require_once 'w8io_config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = 'GENERATORS/64/64';

$uri = explode( '/', $uri );

function flt( $string )
{
    $filter = preg_filter( '/[^a-zA-Z0-9_.@\-]+/', '', $string );
    return isset( $filter ) ? $filter : $string;
}

$address = flt( $uri[0] );

$f = isset( $uri[1] ) ? flt( $uri[1] ) : false;
$arg = isset( $uri[2] ) ? flt( $uri[2] ) : false;
$arg2 = isset( $uri[3] ) ? flt( $uri[3] ) : false;
$arg3 = isset( $uri[4] ) ? flt( $uri[4] ) : false;
$arg4 = isset( $uri[5] ) ? flt( $uri[5] ) : false;

$light = ( isset( $_COOKIE['light'] ) && (bool)$_COOKIE['light'] ) ? true : false;
if( $address === 'ld' )
{
    $address = '';
    $light = !$light;
    setcookie( 'light', $light, 0x7FFFFFFF, '/' );
}
else
if( $address === 'GENERATORS' )
{
    //$showtime = true;

    if( $f === false )
        $f = 1472;

    $f = intval( $f );
    $n = min( max( $f, isset( $showtime ) ? 1 : 64 ), 100000 );
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
<html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <meta name="format-detection" content="telephone=no">
        <meta name="format-detection" content="date=no">
        <meta name="format-detection" content="address=no">
        <meta name="format-detection" content="email=no">
        <title>w8io%s</title>
        <link rel="shortcut icon" href="%sfavicon.ico" type="image/x-icon">
    </head>
    <style>
        body, table
        {
            font-size: 12pt; font-size: %s; font-family: "Courier New", Courier, monospace;
            background-color: #%s;
            color: #%s;
            border-collapse: collapse;
            overflow-y: scroll;%s
        }
        a
        {
            color: #%s;%s
        }
        hr
        {
            margin: 1em 0 1em 0;
            height: 1px;
            border: 0;
            background-color: #606870;
        }
    </style>
    <body>
        <pre>
', empty( $address ) ? '' : " / $address", W8IO_ROOT,
isset( $showtime ) ? '0.66vw' : '0.90vw',
$bcolor, $tcolor,
isset( $showtime ) ? 'margin: 1em 2em 1em 2em; filter: brightness(144%);' : '',
$tcolor,
isset( $showtime ) ? 'text-decoration: none;' : '' );

if( empty( $address ) )
    $address = 'GENESIS';

require_once './include/w8io_nodes.php';
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

function w8io_print_transactions( $aid, $address, $wtxs, $api, $spam = true )
{
    foreach( $wtxs as $wtx )
    {
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

            $decimals = $info['decimals'];
            $amount = number_format( $amount / pow( 10, $decimals ), $decimals, '.', '' );
            $furl = W8IO_ROOT . "$address/f/{$info['id']}";
            $asset = " <a href=\"$furl\">{$info['name']}</a>";
            $amount = ' ' . ( $b === $aid ? '+' : '-' ) . $amount;
        }
        else if( $b !== - 3 )
        {
            $amount = number_format( $amount / 100000000, 8, '.', '' );
            $furl = W8IO_ROOT . "$address/f/Waves";
            $asset = " <a href=\"$furl\">Waves</a>";
            $amount = ' ' . ( $b === $aid ? '+' : '-' ) . $amount;
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
                $afee = $info['name'];
                $decimals = $info['decimals'];
                $fee = number_format( $fee / pow( 10, $decimals ), $decimals, '.', '' );
                $fee = " ($fee <a href=\"" . W8IO_ROOT . "$address/f/{$info['id']}\">$afee</a> fee)";
            }
            else
            {
                $afee = "Waves";
                $fee = number_format( $fee / 100000000, 8, '.', '' );
                $fee = " ($fee <a href=\"" . W8IO_ROOT . "$address/f/Waves\">Waves</a> fee)";
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

        $wtype = w8io_tx_type( $wtx['type'] );

        $ashow = $isa ? "<b>$a</b>" : $a;
        $bshow = $isb ? "<b>$b</b>" : $b;

        echo "<small>" . date( 'Y.m.d H:i', $wtx['timestamp'] ) ." ({$wtx['block']})</small> (<a href=\"". W8IO_ROOT . "$address/t/{$wtx['type']}\">$wtype</a>) <a href=\"". W8IO_ROOT . $a ."\">$ashow</a> >> <a href=\"". W8IO_ROOT . $b ."\">$bshow</a>$amount$asset$fee" . PHP_EOL;
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

if( $address === 'b' )
{

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
        $balance = str_pad( number_format( $balance / 100000000, 0, '', '.' ), 10, ' ', STR_PAD_LEFT );

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
        $fee = str_pad( number_format( $fee / 100000000, 8, '.', '' ), 14, ' ', STR_PAD_LEFT );

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
    $gentotal = str_pad( number_format( $gentotal / 100000000, 0, '', '.' ), 80, ' ', STR_PAD_LEFT );
    $feetotal = str_pad( number_format( $feetotal / 100000000, 8, '.', '' ), ( isset( $showtime ) ? 48 : 0 ) + 104, ' ', STR_PAD_LEFT );

    echo "<small style=\"font-size: 50%;\"><br></small><b>$ntotal $gentotal $feetotal</b> ($blktotal)" .  PHP_EOL;

    if( isset( $showtime ) )
        for( $i = $n; $i <= 64; $i++ )
            echo PHP_EOL;
}
else
if( $address === 'SUM' )
{
    $balances = $api->get_all_balances();

    $sum = [];

    foreach( $balances as $balance )
    {
        if( $balance['id'] > 0 )
        {
            $values = json_decode( $balance['value'], true, 512, JSON_BIGINT_AS_STRING );

            if( isset( $values[0] ) || isset( $values[W8IO_ASSET_WAVES_LEASED] ) )
            {
                $waves = isset( $values[0] ) ? $values[0] : 0;
                $waves += isset( $values[W8IO_ASSET_WAVES_LEASED] ) ? $values[W8IO_ASSET_WAVES_LEASED] : 0;
                if( $waves >= 100000000000 )
                    $sum[$api->get_address( $balance['id'] )] = $waves;
            }
        }
    }

    arsort( $sum );

    foreach( $sum as $address => $waves )
    {

        $amount = str_pad( number_format( $waves / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );
        $url = W8IO_ROOT . "$address";

        echo "$amount Waves -- <a href=\"$url\">$address</a>" . PHP_EOL;
    }
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
        $wtxs = $api->get_transactions_where( false, $where, 1000 );
        w8io_print_transactions( false, $address, $wtxs, $api, !( $f === 'f' ) );
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

        echo "<a href=\"". W8IO_ROOT . $address ."\">$address</a>$full_address @ $height" . PHP_EOL . PHP_EOL;
        echo '<table><tr><td valign="top"><pre>';

        $tickers = [];
        $unlisted = [];

        if( !isset( $balance[0] ) )
            $balance[0] = 0;

        // WAVES
        {
            $asset = "Waves";
            $amount = str_pad( number_format( $balance[0] / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

            $furl = W8IO_ROOT . "$address/f/Waves";

            $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl, 'b' => $arg === 0 && $f !== 't' ];
        }

        if( isset( $balance[W8IO_ASSET_WAVES_LEASED] ) )
        {
            $amount = $balance[W8IO_ASSET_WAVES_LEASED] + ( isset( $balance[0] ) ? $balance[0] : 0 );

            if( $amount > 100000000000 )
            {
                $asset = "Waves (GENERATOR)";
                $amount = str_pad( number_format( $amount / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

                $furl = W8IO_ROOT . "$address/f/Waves";

                $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl, 'b' => false ];
            }
        }

        foreach( $balance as $asset => $amount )
        {
            if( $asset > 0 )
            {
                $info = $api->get_asset_info( $asset );
                if( isset( $info['scam'] ) )
                    continue;

                $b = $asset === $arg;
                $asset = $info['name'];
                $decimals = $info['decimals'];
                $amount = str_pad( number_format( $amount / pow( 10, $decimals ), $decimals, '.', '' ), 24, ' ', STR_PAD_LEFT );

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

        echo "------------------------------------------&nbsp;" . PHP_EOL;

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

        echo '</pre></td><td valign="top"><pre>';

        if( $f !== 'pay' )
        {
            $wtxs = $api->get_transactions_where( $aid, $where, 1000 );
            w8io_print_transactions( $aid, $address, $wtxs, $api, !( $f === 'f' ) );
        }
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
                $waves_fees = 0;
                $query = $api->get_transactions_query( "SELECT * FROM transactions WHERE block >= $from AND block <= $to AND b = $aid AND type = 0" );
                foreach( $query as $wtx )
                {
                    $wtx = w8io_filter_wtx( $wtx );
                    if( $wtx['asset'] === 0 )
                        $waves_fees += $wtx['amount'];
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
                $mrt_fees = intval( $mrt_fees * $percent / 100 );

                echo "pay ($from .. $to) ($percent %):" . PHP_EOL . PHP_EOL;
                echo str_pad( number_format( $waves_fees / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT ) . " Waves" . PHP_EOL;
                echo str_pad( number_format( $mrt_fees / 100, 2, '.', '' ), 24, ' ', STR_PAD_LEFT ) . " MinersReward" . PHP_EOL;

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

echo '</pre></td></tr></table>'. PHP_EOL . PHP_EOL;
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
echo '</small></div>';
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
