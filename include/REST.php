<?php

namespace w8io;

require_once 'common.php';

class REST
{
    public RO $RO;
    public array $j;

    public function __construct( $RO )
    {
        $this->RO = $RO;
    }

    public function setHeader( $height, $time, $request, $address )
    {
        $this->j['height'] = $height;
        $this->j['time'] = $time;
        $this->j['request'] = $request;
        $this->j['address'] = $address;
    }

    public function setBalance( $asset, $weight, $amount, $name )
    {
        if( !isset( $this->j['balance'] ) )
            $this->j['balance'] = [[ 'asset', 'weight', 'amount', 'name' ]];

        $this->j['balance'][] = [ $asset, $weight, $amount, $name ];
    }

    public function setTxs( $height, $time, $out, $type, $amount, $asset, $assetId, $address, $fee, $feeasset, $feeassetId )
    {
        if( !isset( $this->j['txs'] ) )
            $this->j['txs'] = [[ 'height', 'time', 'out', 'type', 'amount', 'asset', 'assetId', 'address', 'fee', 'feeasset', 'feeassetId' ]];

        $this->j['txs'][] = [ $height, $time, $out, $type, $amount, $asset, $assetId, $address, $fee, $feeasset, $feeassetId ];
    }

    public function setTxsPagination( $txsNext )
    {
        $this->j['txsNext'] = $txsNext;
    }
}
