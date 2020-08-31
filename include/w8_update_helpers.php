<?php

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

function update_scam( $transactions )
{
    $scam_file = W8IO_DB_DIR . 'scam.txt';

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

    $wks = new deemru\WavesKit();
    $wks->setNodeAddress( 'https://raw.githubusercontent.com' );
    $fresh_scam = $wks->fetch( '/wavesplatform/waves-community/master/Scam%20tokens%20according%20to%20the%20opinion%20of%20Waves%20Community.csv' );
    if( $fresh_scam === false )
    {
        w8io_warning( 'fresh_scam->get() failed' );
        return;
    }

    $scam = explode( "\n", $fresh_scam );
    $scam = array_unique( $scam );
    $fresh_scam = implode( "\n", $scam );

    $mark_scam = array_diff( $scam, $last_scam );
    $unset_scam = array_diff( $last_scam, $scam );

    foreach( $mark_scam as $scamid )
        if( !empty( $scamid ) )
            $transactions->mark_scam( $scamid, true );

    foreach( $unset_scam as $scamid )
        if( !empty( $scamid ) )
            $transactions->mark_scam( $scamid, false );

    file_put_contents( $scam_file, $fresh_scam );
}

