<?php

require_once 'w8io_config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = '3PAWwWa6GbwcJaFzwqXQN5KQm7H96Y7SHTQ';

$uri = explode( '/', $uri );

function flt( $string )
{
    $filter = preg_filter( '/[^a-zA-Z0-9_@\-]+/', '', $string );
    return isset( $filter ) ? $filter : $string;
}

$address = flt( $uri[0] );

$f = isset( $uri[1] ) ? flt( $uri[1] ) : false;
$arg = isset( $uri[2] ) ? flt( $uri[2] ) : false;
$arg2 = isset( $uri[3] ) ? flt( $uri[3] ) : false;
$arg3 = isset( $uri[4] ) ? flt( $uri[4] ) : false;
$arg4 = isset( $uri[5] ) ? flt( $uri[5] ) : false;

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
            font-size: 12pt; font-size: 0.90vw; font-family: "Courier New", Courier, monospace;
            background-color: #404840;
            color: #A0A8C0;
            overflow-y: scroll;
        }
        a
        {
            color: #A0A8C0;
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
', empty( $address ) ? '' : " / $address", W8IO_ROOT );

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
        $asset = $wtx['asset'];
        $amount = $wtx['amount'];

        if( $asset )
        {
            $info = $api->get_asset_info( $asset );
            if( $spam && isset( $info['scam'] ) )
                continue;

            $asset = "<a href=\"" . W8IO_ROOT . "$address/f/{$info['id']}\">{$info['name']}</a>";
            $decimals = $info['decimals'];
            $amount = number_format( $amount / pow( 10, $decimals ), $decimals, '.', '' );
            $furl = W8IO_ROOT . "$address/f/{$info['id']}";
        }
        else
        {
            $asset = "<a href=\"" . W8IO_ROOT . "$address/f/Waves\">Waves</a>";
            $amount = number_format( $amount / 100000000, 8, '.', '' );
            $furl = W8IO_ROOT . "$address/f/Waves";
        }

        $a = (int)$wtx['a'];
        $b = (int)$wtx['b'];

        $amount = ( $b === $aid ? '+' : '-' ) . $amount;
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

            if( isset( $data['b'] ) )
                $b = $api->get_data( $data['b'] );
        }

        $type = w8io_tx_type( $wtx['type'] );

        $ashow = $isa ? "<b>$a</b>" : $a;
        $bshow = $isb ? "<b>$b</b>" : $b;

        echo "<small>" . date( 'Y.m.d H:i', $wtx['timestamp'] ) ." ({$wtx['block']})</small> (<a href=\"". W8IO_ROOT . "$address/t/{$wtx['type']}\">$type</a>) <a href=\"". W8IO_ROOT . $a ."\">$ashow</a> >> <a href=\"". W8IO_ROOT . $b ."\">$bshow</a> $amount $asset$fee" . PHP_EOL;
    }
}

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

        {
            $asset = "Waves";
            $amount = str_pad( number_format( $balance[0] / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

            $furl = W8IO_ROOT . "$address/f/Waves";

            $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
        }

        if( isset( $balance[W8IO_ASSET_WAVES_LEASED] ) )
        {
            $amount = $balance[W8IO_ASSET_WAVES_LEASED] + ( isset( $balance[0] ) ? $balance[0] : 0 );

            if( $amount > 100000000000 )
            {
                $asset = "Waves (GENERATOR)";
                $amount = str_pad( number_format( $amount / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

                $furl = W8IO_ROOT . "$address/f/Waves";

                $tickers[] = $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];
            }
        }

        foreach( $balance as $asset => $amount )
        {
            if( $asset > 0 )
            {
                $info = $api->get_asset_info( $asset );
                if( isset( $info['scam'] ) )
                    continue;

                $asset = $info['name'];
                $decimals = $info['decimals'];
                $amount = str_pad( number_format( $amount / pow( 10, $decimals ), $decimals, '.', '' ), 24, ' ', STR_PAD_LEFT );

                $furl = W8IO_ROOT . "$address/f/{$info['id']}";

                $record = [ 'asset' => $asset, 'amount' => $amount, 'furl' => $furl ];

                if( isset( $info['ticker'] ) )
                    $tickers[] = $record;
                else
                    $unlisted[] = $record;
            }
        }

        foreach( $tickers as $record )
            echo "{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>" . PHP_EOL;

        echo "------------------------------------------&nbsp;" . PHP_EOL;

        foreach( $unlisted as $record )
            echo "{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>" . PHP_EOL;

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
                $waves_fees *= $percent / 100;

                // mrt_fees
                $mrt_fees = 0;
                $mrt_id = $api->get_asset( '4uK8i4ThRGbehENwa6MxyLtxAjAo1Rj9fduborGExarC' );
                $query = $api->get_transactions_query( "SELECT * FROM transactions WHERE block >= $from AND block <= $to AND b = $aid AND type = 4 AND asset = $mrt_id" );
                foreach( $query as $wtx )
                {
                    $wtx = w8io_filter_wtx( $wtx );
                    $mrt_fees += $wtx['amount'];
                }
                $mrt_fees *= $percent / 100;

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

echo '<hr><div width="100%" align="right"><small>';
if( file_exists( '.git/FETCH_HEAD' ) )
{
    $rev = file_get_contents( '.git/FETCH_HEAD', null, null, 0, 40 );
    echo "<a href=\"https://github.com/deemru/w8io\">github/deemru/w8io</a>/<a href=\"https://github.com/deemru/w8io/commit/$rev\">" . substr( $rev, 0, 7 ) . '</a>';
}
echo PHP_EOL . sprintf( '%.02f ms', 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] ) );
if( defined( 'W8IO_ANALYTICS' ) )
    echo '<br><br>' . W8IO_ANALYTICS;
echo '</small></div>';
?>

        </pre>
    </body>
</html>
