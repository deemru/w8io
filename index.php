<?php

require_once 'w8io_config.php';

if( isset( $_SERVER['REQUEST_URI'] ) )
    $uri = substr( $_SERVER['REQUEST_URI'], strlen( W8IO_ROOT ) );
else
    $uri = '3PAWwWa6GbwcJaFzwqXQN5KQm7H96Y7SHTQ';

$uri = explode( '/', $uri );

$address = $uri[0];

$f = isset( $uri[1] ) ? $uri[1] : false;
$arg = isset( $uri[2] ) ? $uri[2] : false;

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
    </style>
    <body>
        <pre>
', " / $address", W8IO_ROOT );

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

function w8io_print_transactions( $api )
{
    $aid = false;
    $address = '3PAWwWa6GbwcJaFzwqXQN5KQm7H96Y7SHT1';
    $wtxs = $api->get_address_transactions( false, false, 10000 );

    if( $wtxs === false )
        w8io_error( "get_address_transactions( $aid ) failed" );

    echo '</pre></td><td valign="top"><pre>';
    echo 'transactions:' . PHP_EOL;
    foreach( $wtxs as $wtx )
    {
        if( $wtx['type'] == 11 )
            continue;

        $asset = $wtx['asset'];
        $amount = $wtx['amount'];

        if( $asset )
        {
            $info = $api->get_asset_info( $asset );
            if( isset( $info['scam'] ) )
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

        $amount = ( $b == $aid ? '+' : '-' ) . $amount;
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

        echo "    <small>" . date( 'Y.m.d H:i:s', $wtx['timestamp'] ) ."</small> ($type) <a href=\"". W8IO_ROOT . $a ."\">$ashow</a> >> <a href=\"". W8IO_ROOT . $b ."\">$bshow</a> $amount $asset$fee" . PHP_EOL;
    }
}

$aid = $api->get_aid( $address );
if( $aid === false )
{
    w8io_trace( 'w', "\"$address\" not found" );
    w8io_print_transactions( $api );
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

    echo 'balance:' . PHP_EOL;
    $tickers = array();
    $unlisted = array();

    if( isset( $balance[0] ) )
    {
        $asset = "Waves";
        $amount = str_pad( number_format( $balance[0] / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

        $furl = W8IO_ROOT . "$address/f/Waves";

        $tickers[] = $record = array( 'asset' => $asset, 'amount' => $amount, 'furl' => $furl );
    }

    if( isset( $balance[W8IO_ASSET_WAVES_LEASED] ) )
    {
        $amount = $balance[W8IO_ASSET_WAVES_LEASED] + ( isset( $balance[0] ) ? $balance[0] : 0 );

        if( $amount > 1000 )
        {
            $asset = "Waves (GENERATOR)";
            $amount = str_pad( number_format( $amount / 100000000, 8, '.', '' ), 24, ' ', STR_PAD_LEFT );

            $furl = W8IO_ROOT . "$address/f/Waves";

            $tickers[] = $record = array( 'asset' => $asset, 'amount' => $amount, 'furl' => $furl );
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

            $record = array( 'asset' => $asset, 'amount' => $amount, 'furl' => $furl );

            if( isset( $info['ticker'] ) )
                $tickers[] = $record;
            else
                $unlisted[] = $record;
        }
    }

    foreach( $tickers as $record )
        echo "{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>" . PHP_EOL;

    echo "------------------------------------------" . PHP_EOL;

    foreach( $unlisted as $record )
        echo "{$record['amount']} <a href=\"{$record['furl']}\">{$record['asset']}</a>" . PHP_EOL;


    if( $f === 'f' )
        $wtxs = $api->get_address_transactions_asset( $aid, $height, $arg, 1000 );
    else
        $wtxs = $api->get_address_transactions( $aid, $height, 1000 );

    if( $wtxs === false )
        w8io_error( "get_address_transactions( $aid ) failed" );

    echo '</pre></td><td valign="top"><pre>';
    echo 'transactions:' . PHP_EOL;
    foreach( $wtxs as $wtx )
    {
        $asset = $wtx['asset'];
        $amount = $wtx['amount'];

        if( $asset )
        {
            $info = $api->get_asset_info( $asset );
            if( isset( $info['scam'] ) )
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

        $amount = ( $b == $aid ? '+' : '-' ) . $amount;
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

        echo "    <small>" . date( 'Y.m.d H:i:s', $wtx['timestamp'] ) ."</small> ($type) <a href=\"". W8IO_ROOT . $a ."\">$ashow</a> >> <a href=\"". W8IO_ROOT . $b ."\">$bshow</a> $amount $asset$fee" . PHP_EOL;
    }
}

echo '</pre></td></tr></table>'. PHP_EOL . PHP_EOL;

echo '<small>';
if( file_exists( '.git/FETCH_HEAD' ) )
{
    $rev = file_get_contents( '.git/FETCH_HEAD', null, null, 0, 40 );
    echo "<a href=\"https://github.com/deemru/w8io\">github/deemru/w8io</a>/<a href=\"https://github.com/deemru/w8io/commit/$rev\">" . substr( $rev, 0, 7 ) . '</a>';
}
echo PHP_EOL . sprintf( '%.02f ms', 1000 * ( microtime( true ) - $_SERVER['REQUEST_TIME_FLOAT'] ) );
echo '</small>';
?>

        </pre>
    </body>
</html>
