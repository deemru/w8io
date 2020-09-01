<?php

namespace w8io;

require_once 'common.php';
require_once 'RO.php';

use deemru\Pairs;
use deemru\Triples;
use deemru\KV;

class BlockchainParser
{
    public Triples $db;
    public Triples $pts;
    public KV $kvAddresses;
    public KV $kvAliases;
    public KV $kvAliasInfo;
    public KV $kvAssets;
    public KV $kvAssetInfo;
    public KV $kvGroups;
    public KV $kvFunctions;
    public KV $sponsorships;
    public Blockchain $blockchain;
    public BlockchainParser $parser;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->pts = new Triples( $this->db , 'pts', 1,
            // uid                 | txkey    | type     | a        | b        | asset    | amount   | feeasset | fee      | addon    | group
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7       | r8       | r9       | r10
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         1,         1,         1,         1,         0,         0,         0,         0,         1 ] );

        $this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r3_r2_index ON pts( r3, r2 )' );
        $this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )' );
        $this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r3_r5_index ON pts( r3, r5 )' );
        $this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r4_r5_index ON pts( r4, r5 )' );

        $this->RO = new RO( $this->db );
        $this->balances = new BlockchainBalances( $this->db );

        $this->kvAddresses =     ( new KV( true )  )->setStorage( $this->db, 'addresses', true );
        $this->kvAliases =       ( new KV( true ) )->setStorage( $this->db, 'aliases', true );
        $this->kvAliasInfo =     ( new KV( false ) )->setStorage( $this->db, 'aliasInfo', true, 'INTEGER PRIMARY KEY', 'INTEGER' );
        $this->kvAssets =        ( new KV( true )  )->setStorage( $this->db, 'assets', true );
        $this->kvAssetInfo =     ( new KV( false ) )->setStorage( $this->db, 'assetInfo', true, 'INTEGER PRIMARY KEY', 'TEXT' );
        $this->kvGroups =        ( new KV( true ) )->setStorage( $this->db, 'groups', true );
        $this->kvFunctions =     ( new KV( true ) )->setStorage( $this->db, 'functions', true );

        $this->sponsorships = new KV;

        $this->kvs = [
            $this->kvAddresses,
            $this->kvAliases,
            $this->kvAliasInfo,
            $this->kvAssets,
            $this->kvAssetInfo,
            $this->kvGroups,
            $this->kvFunctions,
        ];

        $this->setHighs();
        $this->recs = [];
        $this->workpts = [];
        $this->workheight = -1;
        $this->resetMTS();
    }

    public function resetMTS()
    {
        $this->mts = [];
        for( $i = TX_SPONSOR; $i <= TX_UPDATE_ASSET_INFO; ++$i )
            $this->mts[$i] = 0;
    }

    public function printMTS()
    {
        for( $i = TX_SPONSOR; $i <= TX_UPDATE_ASSET_INFO; ++$i )
            wk()->log( sprintf( "%d) %.2f ms", $i, $this->mts[$i] * 1000 ) );
    }

    public function cacheHalving()
    {
        foreach( $this->kvs as $kv )
            $kv->cacheHalving();
    }

    private function setSponsorship( $asset, $sponsorship )
    {
        $this->sponsorships->setKeyValue( $asset, $sponsorship );
    }

    private function getSponsorship( $asset )
    {
        $sponsorship = $this->sponsorships->getValueByKey( $asset );
        if( $sponsorship !== false )
            return $sponsorship;

        if( !isset( $this->q_getSponsorship ) )
        {
            $this->q_getSponsorship = $this->pts->db->prepare( 'SELECT * FROM pts WHERE r10 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->q_getSponsorship === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->q_getSponsorship->execute( [ $this->getGroupSponsorship( $asset ) ] ) === false )
            w8_err( __FUNCTION__ );

        $ts = $this->q_getSponsorship->fetchAll();
        if( $ts === false )
            w8_err( __FUNCTION__ );
         
        if( isset( $ts[0] ) && $ts[0][AMOUNT] !== '0' )
            $sponsorship = $ts[0];
        else
            $sponsorship = 0;

        $this->setSponsorship( $asset, $sponsorship );
        return $sponsorship;
    }

    private function getLeaseInfoById( $id )
    {
        $txkey = $this->RO->getTxKeyById( $id );
        if( $txkey === false )
            w8_err( "getLeaseInfoById: getTxKeyById( $id )" );

        foreach( $this->recs as $ts )
            if( $ts[TXKEY] === $txkey && $ts[TYPE] === TX_LEASE )
                return $ts;

        if( !isset( $this->q_getLeaseInfoById ) )
        {
            $this->q_getLeaseInfoById = $this->pts->db->prepare( 'SELECT * FROM pts WHERE r1 = ? GROUP BY r2 HAVING r2 = ' . TX_LEASE );
            if( $this->q_getLeaseInfoById === false )
                w8io_error( "getLeaseInfoById" );
        }

        if( $this->q_getLeaseInfoById->execute( [ $txkey ] ) === false )
            w8io_error( "getLeaseInfoById( $id )" );

        $ts = $this->q_getLeaseInfoById->fetchAll();
        if( $ts === false )
            w8io_error( "getLeaseInfoById( $id )" );

        return $ts[0];
    }

    public function setHighs()
    {
        $this->setUid();
    }

    private function setUid()
    {
        if( false === ( $this->uid = $this->pts->getHigh( 0 ) ) )
            $this->uid = 0;
    }

    private function getNewUid()
    {
        return ++$this->uid;
    }

    private function getSenderId( $address, $tx = null )
    {
        $id = $this->kvAddresses->getKeyByValue( $address );
        if( $id === false )
        {
            if( !isset( $tx ) )
                w8_err( __FUNCTION__ );

            if( !isset( $tx['stateChanges']['transfers'][0]['address'] ) ||
                $address !== $tx['stateChanges']['transfers'][0]['address'] )
                w8_err( __FUNCTION__ );

            $this->getRecipientId( $address );
            return $this->getSenderId( $address );
        }
        
        return $id;
    }

    private function getRecipientId( $addressOrAlias )
    {
        if( $addressOrAlias[0] === '3' && strlen( $addressOrAlias ) === 35 )
            return $this->kvAddresses->getForcedKeyByValue( $addressOrAlias );

        if( substr( $addressOrAlias, 0, 6 ) !== 'alias:')
            w8io_error( 'unexpected $addressOrAlias = ' . $addressOrAlias );
        
        $id = $this->kvAliases->getKeyByValue( substr( $addressOrAlias, 8 ) );
        if( $id === false )
            w8io_error( 'getRecipientId' );

        $id = $this->kvAliasInfo->getValueByKey( $id );
        if( $id === false )
            w8io_error( 'getRecipientId' );
        
        return $id;
    }

    private function getFunctionId( $function )
    {       
        return $this->kvFunctions->getForcedKeyByValue( $function );
    }

    private function getAliasId( $alias )
    {
        if( $alias[0] !== 'a' )
            return 0;
        
        $id = $this->kvAliases->getKeyByValue( substr( $alias, 8 ) );
        if( $id === false )
            w8_err( __FUNCTION__ );
        
        return $id;
    }

    private function getNewAssetId( $tx )
    {
        $id = $this->kvAssets->getForcedKeyByValue( $tx['assetId'] );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $this->kvAssetInfo->setKeyValue( $id, $tx['decimals'] . '_' . $name );
        return $id;
    }

    private function getUpdatedAssetId( $tx )
    {
        $id = $this->kvAssets->getKeyByValue( $tx['assetId'] );
        if( $id === false )
            w8_err( __FUNCTION__ );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $info = $this->kvAssetInfo->getValueByKey( $id );
        $this->kvAssetInfo->setKeyValue( $id, substr( $info, 0, 2 ) . $name );
        return $id;
    }

    private function getAssetId( $asset )
    {
        $id = $this->kvAssets->getKeyByValue( $asset );
        if( $id === false )
            w8_err( __FUNCTION__ );
        
        return $id;
    }

    private function applySponsorship( $txkey, &$tx )
    {
        if( $tx['feeAssetId'] === null )
        {
            $tx[FEEASSET] = WAVES_ASSET;
            $tx[FEE] = $tx['fee'];
            return;
        }

        $afee = $this->getAssetId( $tx['feeAssetId'] );
        $sponsorship = $this->getSponsorship( $afee );
        if( $sponsorship !== 0 && w8k2h( $txkey ) >= GetHeight_Sponsorship() )
        {
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_SPONSOR,
                A =>        $this->getSenderId( $tx['sender'] ),
                B =>        (int)$sponsorship[A],
                ASSET =>    $afee,
                AMOUNT =>   $tx['fee'],
                FEEASSET => WAVES_ASSET,
                FEE =>      gmp_intval( gmp_div( gmp_mul( $tx['fee'], 100000 ), (int)$sponsorship[AMOUNT] ) ),
                ADDON =>    0,
                GROUP =>    0,
            ] );

            $tx[FEEASSET] = SPONSOR_ASSET;
            $tx[FEE] = 0;
            return;
        }

        $tx[FEEASSET] = $afee;
        $tx[FEE] = $tx['fee'];
    }

    private function getPTS( $from, $to )
    {
        if( !isset( $this->q_getPTS ) )
        {
            $this->q_getPTS = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 >= ? AND r1 < ?" );
            if( $this->q_getPTS === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->q_getPTS->execute( [ $from, $to ] ) === false )
            w8_err( __FUNCTION__ );

        return $this->q_getPTS->fetchAll();
    }

    private function getPTSat( $height )
    {
        return $this->getPTS( w8h2k( $height ), w8h2k( $height + 1 ) - 1 );
    }

    private function getFeesAt( $height, $reward )
    {
        $fees = [ WAVES_ASSET => $reward ];
        $ngfees = [];

        if( $this->workheight === $height )
            $pts = $this->workpts;
        else
        {
            $this->flush();
            $pts = $this->getPTSat( $height );
        }

        foreach( $pts as $ts )
        {
            $fee = (int)$ts[FEE];
            if( $fee <= 0 )
                continue;

            if( (int)$ts[TYPE] === TX_EXCHANGE ) // TX_MATCHER pays real fees
                continue;

            $feeasset = (int)$ts[FEEASSET];

            if( $height >= GetHeight_NG() )
            {
                $ngfee = intdiv( $fee, 5 ) * 2;
                $fees[$feeasset] = $ngfee + ( isset( $fees[$feeasset] ) ? $fees[$feeasset] : 0 );
                $ngfees[$feeasset] = $fee - $ngfee + ( isset( $ngfees[$feeasset] ) ? $ngfees[$feeasset] : 0 );
            }
            else
            {
                $fees[$feeasset] = $fee + ( isset( $fees[$feeasset] ) ? $fees[$feeasset] : 0 );
            }
        }

        if( $height > GetHeight_NG() )
            foreach( $this->getNGFeesAt( $height - 1 ) as $feeasset => $fee )
                if( $fee > 0 )
                    $fees[$feeasset] = $fee + ( isset( $fees[$feeasset] ) ? $fees[$feeasset] : 0 );

        $this->lastfees = [ $height, $ngfees ];
        return [ $fees, $ngfees ];
    }

    public function getNGFeesAt( $height )
    {
        if( isset( $this->lastfees ) && $this->lastfees[0] === $height )
            return $this->lastfees[1];

        if( !isset( $this->q_getNGFeesAt ) )
        {
            $this->q_getNGFeesAt = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 == ?" );
            if( $this->q_getNGFeesAt === false )
                w8io_error( 'getNGFeesAt' );
        }

        if( $this->q_getNGFeesAt->execute( [ w8h2k( $height + 1 ) - 1 ] ) === false )
            w8io_error( 'getNGFeesAt' );

        $pts = $this->q_getNGFeesAt->fetchAll();
        if( count( $pts ) < 1 )
            w8_err( "unexpected getNGFeesAt( $height )" );

        $ngfees = [];
        foreach( $pts as $ts )
            $ngfees[(int)$ts[ASSET]] = (int)$ts[ADDON];
    
        return $ngfees;
    }

    private function appendTS( $ts )
    {
        $this->recs[] = $ts;
        $this->workpts[] = $ts;
    }

    private function processGeneratorTransaction( $txkey, $tx )
    {
        list( $fees, $ngfees ) = $this->getFeesAt( w8k2h( $txkey ), $tx['reward'] );

        foreach( $fees as $feeasset => $fee )
        {
            $this->recs[] = [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_GENERATOR,
                A =>        GENERATOR,
                B =>        $this->getRecipientId( $tx['generator'] ),
                ASSET =>    $feeasset,
                AMOUNT =>   $fee,
                FEEASSET => 0,
                FEE =>      0,
                ADDON =>    isset( $ngfees[$feeasset] ) ? $ngfees[$feeasset] : 0,
                GROUP =>    0,
            ];
        }

        $this->workheight = w8k2h( $txkey ) + 1;
        $this->workpts = [];
    }

    private function processFailedTransaction( $txkey, $tx )
    {
        switch( $tx['type'] )
        {
            case TX_INVOKE:
                $this->appendTS( [
                    UID =>      $this->getNewUid(),
                    TXKEY =>    $txkey,
                    TYPE =>     TX_INVOKE,
                    A =>        $this->getSenderId( $tx['sender'] ),
                    B =>        $this->getRecipientId( $tx['dApp'] ),
                    ASSET =>    WAVES_ASSET,
                    AMOUNT =>   0,
                    FEEASSET => $tx[FEEASSET],
                    FEE =>      $tx[FEE],
                    ADDON =>    0,
                    GROUP =>    FAILED_GROUP,
                ] );
                break;
            default:
                w8_err( 'unknown failed transaction ' . $tx['type'] );
        }
    }

    private function processGenesisTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_GENESIS,
            A =>        GENESIS,
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => WAVES_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processPaymentTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_PAYMENT,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => WAVES_ASSET,
            FEE =>      $tx['fee'],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processIssueTransaction( $txkey, $tx, $dApp = null )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_ISSUE,
            A =>        isset( $dApp ) ? $dApp : $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getNewAssetId( $tx ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => isset( $dApp ) ? INVOKE_ASSET : $tx[FEEASSET],
            FEE =>      isset( $dApp ) ? 0 : $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processReissueTransaction( $txkey, $tx, $dApp = null )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_REISSUE,
            A =>        isset( $dApp ) ? $dApp : $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => isset( $dApp ) ? INVOKE_ASSET : $tx[FEEASSET],
            FEE =>      isset( $dApp ) ? 0 : $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processBurnTransaction( $txkey, $tx, $dApp = null )
    {
        $amount = isset( $tx['amount'] ) ? $tx['amount'] : $tx['quantity'];

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_BURN,
            A =>        isset( $dApp ) ? $dApp : $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $amount,
            FEEASSET => isset( $dApp ) ? INVOKE_ASSET : $tx[FEEASSET],
            FEE =>      isset( $dApp ) ? 0 : $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processExchangeTransaction( $txkey, $tx )
    {
        if( $tx['version'] >= 3 )
            w8io_error();

        if( isset( $tx['feeAssetId'] ) )
            w8io_error();

        if( $tx['order1']['orderType'] === 'buy' )
        {
            $buyer = $tx['order1'];
            $seller = $tx['order2'];
        }
        else
        {
            $buyer = $tx['order2'];
            $seller = $tx['order1'];
        }

        $ba = $this->getSenderId( $buyer['sender'] );
        $sa = $this->getSenderId( $seller['sender'] );

        $basset = $buyer['assetPair']['amountAsset'];
        $basset = isset( $basset ) ? $this->getAssetId( $basset ) : WAVES_ASSET;
        $sasset = $buyer['assetPair']['priceAsset'];
        $sasset = isset( $sasset ) ? $this->getAssetId( $sasset ) : WAVES_ASSET;

        $bfee = $tx['buyMatcherFee'];
        $sfee = $tx['sellMatcherFee'];
        $fee = $tx['fee'];
        $bafee = isset( $buyer['matcherFeeAssetId'] ) ? $this->getAssetId( $buyer['matcherFeeAssetId'] ) : WAVES_ASSET;
        $safee = isset( $seller['matcherFeeAssetId'] ) ? $this->getAssetId( $seller['matcherFeeAssetId'] ) : WAVES_ASSET;
        $afee = isset( $tx['feeAssetId'] ) ? $this->getAssetId( $tx['feeAssetId'] ) : 0;
        
        if( $buyer['version'] >= 4 )
            w8io_error();
        if( $seller['version'] >= 4 )
            w8io_error();

        // MATCHER;
        $diff = [];
        $diff[$bafee] = $bfee;
        $diff[$safee] = $sfee + ( isset( $diff[$safee] ) ? $diff[$safee] : 0 );
        $diff[$afee] = -$fee + ( isset( $diff[$afee] ) ? $diff[$afee] : 0 );
        foreach( $diff as $masset => $mamount )
        {
            if( $masset === $afee )
            {
                $this->appendTS( [
                    UID =>      $this->getNewUid(),
                    TXKEY =>    $txkey,
                    TYPE =>     TX_MATCHER,
                    A =>        MATCHER,
                    B =>        $this->getRecipientId( $tx['sender'] ),
                    ASSET =>    $masset,
                    AMOUNT =>   $mamount,
                    FEEASSET => $afee,
                    FEE =>      $fee,
                    ADDON =>    0,
                    GROUP =>    0,
                ] );
            }
            else if( $mamount )
            {
                $this->appendTS( [
                    UID =>      $this->getNewUid(),
                    TXKEY =>    $txkey,
                    TYPE =>     TX_MATCHER,
                    A =>        MATCHER,
                    B =>        $this->getRecipientId( $tx['sender'] ),
                    ASSET =>    $masset,
                    AMOUNT =>   $mamount,
                    FEEASSET => 0,
                    FEE =>      0,
                    ADDON =>    0,
                    GROUP =>    0,
                ] );
            }
        }

        // price + group
        {
            /*
            $bdecimals = (int)$this->kvAssetInfo->getValueByKey( $basset )[0];
            $sdecimals = (int)$this->kvAssetInfo->getValueByKey( $sasset )[0];

            $price = (string)$tx['price'];
            if( $bdecimals !== 8 )
                $price = substr( $price, 0, -8 + $bdecimals );

            if( $sdecimals )
            {
                if( strlen( $price ) <= $sdecimals )
                    $price = str_pad( $price, $sdecimals + 1, '0', STR_PAD_LEFT );
                $price = substr_replace( $price, '.', -$sdecimals, 0 );
            }
            $price = $price . ' ' . $bname . '/' . $sname;
            $wtx['data'] = [ 'p' => $this->get_dataid( $price, true ) ];
            */
        }

        $amount = $tx['amount'];
        $price = $tx['price'];
        $group = $this->getGroupExchange( $basset, $sasset, true );

        // SELLER -> BUYER
        {
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_EXCHANGE,
                A =>        $sa,
                B =>        $ba,
                ASSET =>    $basset,
                AMOUNT =>   $amount,
                FEEASSET => $safee,
                FEE =>      $sfee,
                ADDON =>    $price,
                GROUP =>    $group,
            ] );
        }
        // BUYER -> SELLER
        {
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_EXCHANGE,
                A =>        $ba,
                B =>        $sa,
                ASSET =>    $sasset,
                AMOUNT =>   gmp_intval( gmp_div( gmp_mul( $price, $amount ), 100000000 ) ),
                FEEASSET => $bafee,
                FEE =>      $bfee,
                ADDON =>    $price,
                GROUP =>    $group,
            ] );
        }
    }

    private function processTransferTransaction( $txkey, $tx, $dApp = null )
    {
        if( isset( $dApp ) )
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $dApp,
            B =>        $this->getRecipientId( $tx['address'] ),
            ASSET =>    isset( $tx['asset'] ) ? $this->getAssetId( $tx['asset'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => INVOKE_ASSET,
            FEE =>      0,
            ADDON =>    $this->getAliasId( $tx['address'] ),
            GROUP =>    0,
        ] );
        else
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    0,
        ] );
    }

    private function processLeaseTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    0,
        ] );
    }

    private function processLeaseCancelTransaction( $txkey, $tx )
    {
        $ts = $this->getLeaseInfoById( $tx['leaseId'] );
        if( $ts === false )
            w8io_error();

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE_CANCEL,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        (int)$ts[B],
            ASSET =>    (int)$ts[ASSET],
            AMOUNT =>   (int)$ts[AMOUNT],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    (int)$ts[ADDON],
            GROUP =>    0,
        ] );
    }

    private function processAliasTransaction( $txkey, $tx )
    {
        $a = $this->getSenderId( $tx['sender'] );
        $id = $this->kvAliases->getForcedKeyByValue( $tx['alias'] );
        $this->kvAliasInfo->setKeyValue( $id, $a );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_ALIAS,
            A =>        $a,
            B =>        UNDEFINED,
            ASSET =>    0,
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $id,
            GROUP =>    0,
        ] );
    }

    private function processMassTransferTransaction( $txkey, $tx )
    {
        $a = $this->getSenderId( $tx['sender'] );
        $asset = isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET;

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_MASS_TRANSFER,
            A =>        $a,
            B =>        MASS,
            ASSET =>    $asset,
            AMOUNT =>   $tx['totalAmount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );

        foreach( $tx['transfers'] as $mtx )
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_MASS_TRANSFER,
                A =>        $a,
                B =>        $this->getRecipientId( $mtx['recipient'] ),
                ASSET =>    $asset,
                AMOUNT =>   $mtx['amount'],
                FEEASSET => 0,
                FEE =>      0,
                ADDON =>    $this->getAliasId( $mtx['recipient'] ),
                GROUP =>    0,
            ] );
    }

    private function processDataTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_DATA,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    0,
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processSmartAccountTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_SMART_ACCOUNT,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    0,
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processSmartAssetTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_SMART_ASSET,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function getGroupSponsorship( $asset, $new = false )
    {
        $groupName = 's' . $asset;
        return $new ? $this->kvGroups->getForcedKeyByValue( $groupName ) : $this->kvGroups->getKeyByValue( $groupName );
    }

    private function getGroupExchange( $basset, $sasset, $new = false )
    {
        $groupName = 'x' . $basset . '/' . $sasset;
        return $new ? $this->kvGroups->getForcedKeyByValue( $groupName ) : $this->kvGroups->getKeyByValue( $groupName );
    }

    private function getGroupInvoke( $aid, $function, $new = false )
    {
        $groupName = 'i' . $aid . '/' . $function;
        return $new ? $this->kvGroups->getForcedKeyByValue( $groupName ) : $this->kvGroups->getKeyByValue( $groupName );
    }

    private function processSponsorshipTransaction( $txkey, $tx, $dApp = null )
    {
        $asset = $this->getAssetId( $tx['assetId'] );
        $group = $this->getGroupSponsorship( $asset, true );

        $ts = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_SPONSORSHIP,
            A =>        isset( $dApp ) ? $dApp : $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $tx['minSponsoredAssetFee'],
            FEEASSET => isset( $dApp ) ? INVOKE_ASSET : $tx[FEEASSET],
            FEE =>      isset( $dApp ) ? 0 : $tx[FEE],
            ADDON =>    0,
            GROUP =>    $group,
        ];

        $this->setSponsorship( $ts[ASSET], $ts );
        $this->appendTS( $ts );
    }

    private function processInvokeTransaction( $txkey, $tx )
    {
        if( !isset( $tx['stateChanges'] ) )
            w8io_error( "getStateChanges( {$tx['id']} ) failed" );

        if( isset( $tx['payment'][0] ) )
        {
            $payment = $tx['payment'][0];
            $asset = isset( $payment['assetId'] ) ? $this->getAssetId( $payment['assetId'] ) : WAVES_ASSET;
            $amount = $payment['amount'];
        }
        else
        {
            $asset = WAVES_ASSET;
            $amount = 0;
        }

        $sender = $this->getSenderId( $tx['sender'], $tx );
        $dApp = $this->getRecipientId( $tx['dApp'] );
        $addon = $this->getAliasId( $tx['dApp'] );
        $function = $this->getFunctionId( isset( $tx['call'] ) ? $tx['call']['function'] : 'default' );
        $group = $this->getGroupInvoke( $dApp, $function );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_INVOKE,
            A =>        $sender,
            B =>        $dApp,
            ASSET =>    $asset,
            AMOUNT =>   $amount,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $addon,
            GROUP =>    $group,
        ] );

        if( isset( $tx['payment'][1] ) )
        {
            $payment = $tx['payment'][1];
            $asset = isset( $payment['assetId'] ) ? $this->getAssetId( $payment['assetId'] ) : WAVES_ASSET;
            $amount = $payment['amount'];

            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_INVOKE,
                A =>        $sender,
                B =>        $dApp,
                ASSET =>    $asset,
                AMOUNT =>   $amount,
                FEEASSET => 0,
                FEE =>      0,
                ADDON =>    $addon,
                GROUP =>    $group,
            ] );
        }

        if( isset( $tx['payment'][2] ) )
            w8_err( 'unexpected 3rd payment' );

        $stateChanges = $tx['stateChanges'];

        if( w8k2h( $txkey ) >= GetHeight_RideV4() )
        {
            foreach( $stateChanges['issues'] as $itx )
                $this->processIssueTransaction( $txkey, $itx, $dApp );
            foreach( $stateChanges['transfers'] as $itx )
                $this->processTransferTransaction( $txkey, $itx, $dApp );
            foreach( $stateChanges['reissues'] as $itx )
                $this->processReissueTransaction( $txkey, $itx, $dApp );
            foreach( $stateChanges['burns'] as $itx )
                $this->processBurnTransaction( $txkey, $itx, $dApp );
            foreach( $stateChanges['sponsorFees'] as $itx )
                $this->processSponsorshipTransaction( $txkey, $itx, $dApp );
        }
        else
        {
            foreach( $stateChanges['transfers'] as $itx )
                $this->processTransferTransaction( $txkey, $itx, $dApp );
        }
    }

    private function processUpdateAssetInfoTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_UPDATE_ASSET_INFO,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getUpdatedAssetId( $tx ),
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    public function processTransaction( $txkey, $tx )
    {
        $type = $tx['type'];
        if( $type === TX_GENERATOR )
            return $this->processGeneratorTransaction( $txkey, $tx );
        if( $type === TX_GENESIS )
            return $this->processGenesisTransaction( $txkey, $tx );

        $this->applySponsorship( $txkey, $tx );

        if( isset( $tx['applicationStatus'] ) )
            switch( $tx['applicationStatus'] )
            {
                case 'succeeded':
                    break;
                case 'script_execution_failed':
                    return $this->processFailedTransaction( $txkey, $tx );
                default:
                    w8io_error();
            }

        //$tt = microtime( true );

        switch( $type )
        {
            case TX_PAYMENT:
                $this->processPaymentTransaction( $txkey, $tx ); break;
            case TX_ISSUE:
                $this->processIssueTransaction( $txkey, $tx ); break;
            case TX_TRANSFER:
                $this->processTransferTransaction( $txkey, $tx ); break;
            case TX_REISSUE:
                $this->processReissueTransaction( $txkey, $tx ); break;
            case TX_BURN:
                $this->processBurnTransaction( $txkey, $tx ); break;
            case TX_EXCHANGE:
                $this->processExchangeTransaction( $txkey, $tx ); break;
            case TX_LEASE:
                $this->processLeaseTransaction( $txkey, $tx ); break;
            case TX_LEASE_CANCEL:
                $this->processLeaseCancelTransaction( $txkey, $tx ); break;
            case TX_ALIAS:
                $this->processAliasTransaction( $txkey, $tx ); break;
            case TX_MASS_TRANSFER:
                $this->processMassTransferTransaction( $txkey, $tx ); break;
            case TX_DATA:
                $this->processDataTransaction( $txkey, $tx ); break;
            case TX_SMART_ACCOUNT:
                $this->processSmartAccountTransaction( $txkey, $tx ); break;
            case TX_SMART_ASSET:
                $this->processSmartAssetTransaction( $txkey, $tx ); break;
            case TX_SPONSORSHIP:
                $this->processSponsorshipTransaction( $txkey, $tx ); break;
            case TX_INVOKE:
                $this->processInvokeTransaction( $txkey, $tx ); break;
            case TX_UPDATE_ASSET_INFO:
                $this->processUpdateAssetInfoTransaction( $txkey, $tx ); break;
                
            default:
                w8io_error( 'unknown' );
        }

        //$this->mts[$type] += microtime( true ) - $tt;
    }

    private function flush()
    {
        if( count( $this->recs ) )
        {
            $this->pts->merge( $this->recs );
            $this->balances->update( $this->recs );
            $this->recs = [];

            foreach( $this->kvs as $kv )
                $kv->merge();
        }
    }

    public function rollback( $txfrom )
    {
        // BALANCES
        $pts = $this->getPTS( $txfrom, PHP_INT_MAX );
        $this->balances->rollback( $pts );

        // PTS
        $this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );
        $this->sponsorships->reset();
        $this->setHighs();
        $this->workpts = [];
        $this->workheight = -1;
    }

    public function update( $txs )
    {
        // if global start not begin from FULL pts per block
        // append current PTS to track block fee
        foreach( $txs as $txkey => $tx )
            $this->processTransaction( $txkey, $tx );

        //$this->printMTS();
        //$this->resetMTS();

        $this->flush();
    }
}

