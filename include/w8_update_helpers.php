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

    $weights = [];
    $lastTxKey = 0;
    $total = 0;
    foreach( $pts as $ts )
    {
        if( $ts[TYPE] !== '7' )
            continue;
        if( $ts[A] === $ts[B] )
            continue;
        $asset = (int)$ts[ASSET];

        if( $lastTxKey === $ts[TXKEY] )
        {
            if( $asset === 0 )
            {
                $asset = (int)$lastTs[ASSET];
                $amount = (int)$ts[AMOUNT];
            }
            else
            {
                if( (int)$lastTs[ASSET] !== 0 )
                    continue;
                $amount = (int)$lastTs[AMOUNT];
            }
            $weights[$asset] = $amount + ( isset( $weights[$asset] ) ? $weights[$asset] : 0 );
            $total += $amount;
        }
        else
        {
            $lastTxKey = $ts[TXKEY];
            $lastTs = $ts;
        }
    }

    $assets = $parser->kvAssets;
    $assetInfo = $parser->kvAssetInfo;

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

    $mark_tickers = array_diff( $tickers, $last_tickers );
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

