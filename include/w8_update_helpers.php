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
            if( $feature['id'] === 2 && $feature['blockchainStatus'] === 'ACTIVATED' )
                return ( $height = $feature['activationHeight'] + 1 );

    return $height;
}

function GetHeight_RideV4()
{
    static $height;

    if( !isset( $height ) )
    {
        foreach( wk()->json_decode( wk()->fetch( '/activation/status' ) )['features'] as $feature )
            if( $feature['id'] === 15 && ( $feature['blockchainStatus'] === 'ACTIVATED' || $feature['blockchainStatus'] === 'APPROVED' ) )
                return ( $height = $feature['activationHeight'] );
    }

    return $height;
}

function GetHeight_RideV5()
{
    static $height;

    if( !isset( $height ) )
    {
        foreach( wk()->json_decode( wk()->fetch( '/activation/status' ) )['features'] as $feature )
            if( $feature['id'] === 16 && ( $feature['blockchainStatus'] === 'ACTIVATED' || $feature['blockchainStatus'] === 'APPROVED' ) )
                return ( $height = $feature['activationHeight'] );
    }

    return $height;
}

function GetHeight_Sponsorship()
{
    static $height;

    if( !isset( $height ) )
    {
        $json = wk()->json_decode( wk()->fetch( '/activation/status' ) );
        foreach( $json['features'] as $feature )
            if( $feature['id'] === 7 && $feature['blockchainStatus'] === 'ACTIVATED' )
                return ( $height = $feature['activationHeight'] + $json['votingInterval'] );
    }

    return $height;
}

function procResetInfo( $parser )
{
    if( !file_exists( W8IO_DB_DIR . 'scams.txt' ) &&
        !file_exists( W8IO_DB_DIR . 'weights.txt' ) )
    {
        $assets = $parser->kvAssets;
        $assetInfo = $parser->kvAssetInfo;

        $assets->setHigh();
        $high = $assets->high;
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
            w8io_warning( 'file_get_contents() failed' );
            return;
        }

        $last_scam = explode( "\n", $last_scam );
    }
    else
        $last_scam = [];

    $wks = new WavesKit;
    $wks->setNodeAddress( 'https://raw.githubusercontent.com' );
    $fresh_scam = $wks->fetch( '/wavesplatform/waves-community/master/Scam%20tokens%20according%20to%20the%20opinion%20of%20Waves%20Community.csv' );
    if( $fresh_scam === false )
        return wk()->log( 'w', 'OFFLINE: ' . $wks->getNodeAddress() );

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
            w8io_warning( 'file_get_contents() failed' );
            return;
        }

        $last_tickers = json_decode( $last_tickers, true );
        if( $last_tickers === false )
            $last_tickers = [];
    }
    else
        $last_tickers = [];

    $height = $blockchain->height;
    $txheight = w8h2k( $height - 2880 );
    $pts = $parser->db->query( "SELECT * FROM pts WHERE r1 > $txheight" );

    $assets = $parser->kvAssets;
    $assetInfo = $parser->kvAssetInfo;

    $usdn = $assets->getKeyByValue( 'DG2xFkPdDwKUoBkzGAhQtLpSGzfXLiCYPEzeKH2Ad24p' );

    $weights_waves = [];
    $total_waves = 0;
    $weights_usdn = [];
    $total_usdn = 0;

    $lastTxKey = 0;
    $lastAsset = 0;
    $lastAmount = 0;
    $isWaves = true;
    foreach( $pts as $ts )
    {
        if( $ts[TYPE] !== '7' )
            continue;
        if( $ts[A] === $ts[B] )
            continue;

        $txkey = (int)$ts[TXKEY];
        $asset = (int)$ts[ASSET];
        $amount = (int)$ts[AMOUNT];

        if( $lastTxKey === $txkey )
        {
            if( $asset === 0 || $asset === $usdn )
            {
                $isWaves = $asset === 0;
                $asset = $lastAsset;
            }
            else
            {
                if( $lastAsset !== 0 && $lastAsset !== $usdn )
                    continue;

                $isWaves = $lastAsset === 0;
                $amount = $lastAmount;
            }

            if( $isWaves )
            {
                $weights_waves[$asset] = $amount + ( isset( $weights_waves[$asset] ) ? $weights_waves[$asset] : 0 );
                $total_waves += $amount;
            }
            else
            {
                $weights_usdn[$asset] = $amount + ( isset( $weights_usdn[$asset] ) ? $weights_usdn[$asset] : 0 );
                $total_usdn += $amount;
            }                
        }
        else
        {
            $lastTxKey = $txkey;
            $lastAsset = $asset;
            $lastAmount = $amount;
        }
    }

    if( isset( $weights_usdn[0] ) )
    {
        $weights_usdn[$usdn] = $weights_usdn[0] * 2;
        unset( $weights_usdn[0] );
    }

    foreach( $weights_waves as $asset => $weight )
        $weights_waves[$asset] = $weight / $total_waves;
    foreach( $weights_usdn as $asset => $weight )
        $weights_usdn[$asset] = $weight / $total_usdn;

    $weights = [];
    foreach( $weights_waves as $asset => $weight )
    {
        $weights[$asset] = $weight;
        if( isset( $weights_usdn[$asset] ) )
        {
            $weights[$asset] += $weights_usdn[$asset];
            unset( $weights_usdn[$asset] );
        }
    }
    foreach( $weights_usdn as $asset => $weight )
        $weights[$asset] = $weight;

    arsort( $weights );

    $tickers = [];
    $num = 255;
    foreach( $weights as $asset => $weight )
    {
        if( $weight < 0.00001 )
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
        
        if( $info[1] !== chr( 1 ) )
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

