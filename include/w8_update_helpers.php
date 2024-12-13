<?php

namespace w8io;

use deemru\WavesKit;

function GetHeight_LeaseReset()
{
    static $height;

    if( !isset( $height ) )
    {
        switch( W8IO_NETWORK )
        {
            case 'W': return ( $height = 462000 );
            case 'T': return ( $height = 51500 );
            default: return ( $height = 0 );
        }
    }

    return $height;
}

function GetHeight_NG()
{
    static $height;

    if( !isset( $height ) )
        foreach( wk()->json_decode( wk()->fetch( '/activation/status' ) )['features'] as $feature )
            if( $feature['id'] === 2 && in_array( $feature['blockchainStatus'], [ 'ACTIVATED', 'APPROVED' ] ) )
            {
                $height = $feature['activationHeight'] + 1;
                break;
            }

    return $height;
}

function GetTxHeight_RideV4()
{
    static $txheight;

    if( !isset( $txheight ) )
    {
        foreach( wk()->json_decode( wk()->fetch( '/activation/status' ) )['features'] as $feature )
            if( $feature['id'] === 15 && in_array( $feature['blockchainStatus'], [ 'ACTIVATED', 'APPROVED' ] ) )
            {
                $txheight = $feature['activationHeight'];
                $txheight = w8h2k( $txheight );
                break;
            }
    }

    return $txheight;
}

function GetTxHeight_RideV5()
{
    static $txheight;

    if( !isset( $txheight ) )
    {
        foreach( wk()->json_decode( wk()->fetch( '/activation/status' ) )['features'] as $feature )
            if( $feature['id'] === 16 && in_array( $feature['blockchainStatus'], [ 'ACTIVATED', 'APPROVED' ] ) )
            {
                $txheight = $feature['activationHeight'];
                $txheight = w8h2k( $txheight );
                break;
            }
    }

    return $txheight;
}

function GetTxHeight_Sponsorship()
{
    static $txheight;

    if( !isset( $txheight ) )
    {
        $json = wk()->json_decode( wk()->fetch( '/activation/status' ) );
        foreach( $json['features'] as $feature )
            if( $feature['id'] === 7 && in_array( $feature['blockchainStatus'], [ 'ACTIVATED', 'APPROVED' ] ) )
            {
                $txheight = $feature['activationHeight'] + $json['votingInterval'];
                $txheight = w8h2k( $txheight );
                break;
            }
    }

    return $txheight;
}

function procResetInfo( $parser )
{
    if( !file_exists( W8IO_DB_DIR . 'scams.txt' ) &&
        !file_exists( W8IO_DB_DIR . 'weights.txt' ) )
    {
        $assets = $parser->kvAssets;
        $assetInfo = $parser->kvAssetInfo;

        $high = $assets->setHigh();
        for( $i = 1; $i <= $high; ++$i )
        {
            $info = $assetInfo->getValueByKey( $i );
            if( $info === false )
                w8_err();
            {
                $info[1] = chr( 0 );
                $assetInfo->setKeyValue( $i, $info );
            }
        }

        $assetInfo->merge();
    }
}

function procScam( $parser )
{
    $scam_file = W8IO_DB_DIR . 'scams.txt';

    if( file_exists( $scam_file ) )
    {
        if( time() - filemtime( $scam_file ) < 3600 )
            return;

        $last_scam = file_get_contents( $scam_file );
        if( $last_scam === false )
        {
            wk()->log( 'w', 'file_get_contents() failed' );
            return;
        }

        $last_scam = explode( "\n", $last_scam );
    }
    else
        $last_scam = [];

    if( 0 ) // https://github.com/wavesplatform/waves-community
    {
        $wks = new WavesKit;
        $wks->setNodeAddress( 'https://raw.githubusercontent.com' );
        $fresh_scam = $wks->fetch( '/wavesplatform/waves-community/master/Scam%20tokens%20according%20to%20the%20opinion%20of%20Waves%20Community.csv' );
        if( $fresh_scam === false )
            return wk()->log( 'w', 'OFFLINE: ' . $wks->getNodeAddress() );
    }
    else // github last update on Jul 22, 2022
    {
        $fresh_scam = file_get_contents( __DIR__ . '/../var/scam.csv' );
    }

    $scam = explode( "\n", $fresh_scam );
    $scam = array_unique( $scam );
    $fresh_scam = implode( "\n", $scam );

    $mark_scam = array_diff( $scam, $last_scam );
    $unset_scam = array_diff( $last_scam, $scam );

    $assets = $parser->kvAssets;
    $assetInfo = $parser->kvAssetInfo;

    foreach( $mark_scam as $scamid )
        if( !empty( $scamid ) )
        {
            $id = $assets->getKeyByValue( $scamid );
            if( $id === false )
            {
                wk()->log( 'w', 'unknown asset: ' . $scamid );
                continue;
            }
            $info = $assetInfo->getValueByKey( $id );
            if( $info === false )
                w8_err();
            $info[1] = chr( 1 );
            $assetInfo->setKeyValue( $id, $info );
        }

    foreach( $unset_scam as $scamid )
        if( !empty( $scamid ) )
        {
            $id = $assets->getKeyByValue( $scamid );
            if( $id === false )
            {
                wk()->log( 'w', 'unknown asset: ' . $scamid );
                continue;
            }
            $info = $assetInfo->getValueByKey( $id );
            if( $info === false )
                w8_err();
            $info[1] = chr( 0 );
            $assetInfo->setKeyValue( $id, $info );
        }

    file_put_contents( $scam_file, $fresh_scam );
    $assetInfo->merge();
}

function procWeight( $blockchain, $parser )
{
    $tickers_file = W8IO_DB_DIR . 'weights.txt';

    if( file_exists( $tickers_file ) )
    {
        if( time() - filemtime( $tickers_file ) < 3600 )
            return;

        $last_tickers = file_get_contents( $tickers_file );
        if( $last_tickers === false )
        {
            wk()->log( 'w', 'file_get_contents() failed' );
            return;
        }

        $last_tickers = json_decode( $last_tickers, true );
        if( $last_tickers === false )
            $last_tickers = [];
    }
    else
        $last_tickers = [];

    $height = $blockchain->height();
    $txheight = w8h2k( $height - 2880 );
    $pts = $parser->db->query( "SELECT * FROM pts WHERE r1 > $txheight" );

    $assetInfo = $parser->kvAssetInfo;

    $fromto = [];
    $lastAsset = 0;
    $lastAmount = 0;
    $lastTxKey = false;
    foreach( $pts as $ts )
    {
        if( $ts[TYPE] !== TX_EXCHANGE )
            continue;
        if( $ts[A] === $ts[B] )
            continue;

        $txkey = $ts[TXKEY];
        $asset = $ts[ASSET];
        $amount = $ts[AMOUNT];

        if( $lastTxKey === false )
        {
            $lastAsset = $asset;
            $lastAmount = $amount;
            $lastTxKey = $txkey;
            continue;
        }
        else
        if( $lastTxKey === $txkey )
        {
            $fromto[$asset][$lastAsset] = $lastAmount + ( $fromto[$asset][$lastAsset] ?? 0 );
            $fromto[$lastAsset][$asset] = $amount + ( $fromto[$lastAsset][$asset] ?? 0 );
            $lastTxKey = false;
            continue;
        }

        $lastTxKey = false;
    }

    $rates = [ 0 => 1 ];
    foreach( $fromto as $asset => $trades )
    {
        if( $asset === WAVES_ASSET )
            continue;
        $waves_volume = $fromto[$asset][WAVES_ASSET] ?? false;
        if( $waves_volume === false )
            continue;
        $asset_volume = $fromto[WAVES_ASSET][$asset];
        $rate = $waves_volume / $asset_volume;
        $rates[$asset] = $rate;
    }

    $weights = [];
    foreach( $fromto as $asset => $trades )
    {
        foreach( $trades as $price => $volume )
        {
            $rate = $rates[$price] ?? false;
            if( $rate === false )
                continue;

            $weights[$asset] = $volume * $rate + ( $weights[$asset] ?? 0 );
        }
    }

    unset( $weights[WAVES_ASSET] );
    arsort( $weights );

    $tickers = [];
    $num = 255;
    foreach( $weights as $asset => $weight )
    {
        if( $weight < 100000000 )
            break;
        $tickers[$asset] = $num;
        if( $num > 2 )
            $num--;
    }

    $mark_tickers = array_diff_assoc( $tickers, $last_tickers );
    $unset_tickers = array_diff( $last_tickers, $tickers );

    foreach( $mark_tickers as $asset => $num )
    {
        $weight = chr( $num );

        $info = $assetInfo->getValueByKey( $asset );
        if( $info === false )
            w8_err();

        if( $num !== 2 || $info[1] !== chr( 1 ) )
            $info[1] = $weight;

        $assetInfo->setKeyValue( $asset, $info );
    }

    foreach( $unset_tickers as $asset => $num )
    {
        $weight = chr( 0 );

        $info = $assetInfo->getValueByKey( $asset );
        if( $info === false )
            w8_err();

        if( $info[1] !== chr( 1 ) )
            $info[1] = $weight;

        $assetInfo->setKeyValue( $asset, $info );
    }

    file_put_contents( $tickers_file, json_encode( $tickers ) );
    $assetInfo->merge();
}
