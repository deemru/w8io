<?php

namespace w8io;

require_once 'common.php';

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
    public KV $leases;
    public Blockchain $blockchain;
    public BlockchainParser $parser;
    public BlockchainBalances $balances;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->pts = new Triples( $this->db , 'pts', 1,
            // uid                 | txkey    | type     | a        | b        | asset    | amount   | feeasset | fee      | addon    | group
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7       | r8       | r9       | r10
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
          //[ 0,                     1,         1,         1,         1,         1,         0,         0,         0,         0,         1 ] );
            [ 0,                     1,         0,         0,         0,         0,         0,         0,         0,         0,         0 ] );

        // CREATE INDEX pts_r3_index ON pts( r3 )
        // CREATE INDEX pts_r4_index ON pts( r4 )
        // CREATE INDEX pts_r5_index ON pts( r5 )
        // CREATE INDEX pts_r5_index ON pts( r10 )
        //$this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r3_r2_index ON pts( r3, r2 )' );
        //$this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r4_r2_index ON pts( r4, r2 )' );
        //$this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r3_r5_index ON pts( r3, r5 )' );
        //$this->pts->query( 'CREATE INDEX IF NOT EXISTS pts_r4_r5_index ON pts( r4, r5 )' );

        $this->balances = new BlockchainBalances( $this->db );

        $this->kvAddresses =     ( new KV( true )  )->setStorage( $this->db, 'addresses', true );
        $this->kvAliases =       ( new KV( true ) )->setStorage( $this->db, 'aliases', true );
        $this->kvAliasInfo =     ( new KV( false ) )->setStorage( $this->db, 'aliasInfo', true, 'INTEGER PRIMARY KEY', 'INTEGER' );
        $this->kvAssets =        ( new KV( true )  )->setStorage( $this->db, 'assets', true );
        $this->kvAssetInfo =     ( new KV( false ) )->setStorage( $this->db, 'assetInfo', true, 'INTEGER PRIMARY KEY', 'TEXT' );
        $this->kvGroups =        ( new KV( true ) )->setStorage( $this->db, 'groups', true );
        $this->kvFunctions =     ( new KV( true ) )->setStorage( $this->db, 'functions', true );

        $this->sponsorships = new KV;
        $this->leases = new KV;

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
        $this->feerecs = [];
        $this->workheight = -1;
        $this->resetMTS(); // debug only
        $this->indexed = $this->uid !== 0;
        $this->qps = []; // version 3 exchange price multipliers
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

        if( !isset( $this->getSponsorship ) )
        {
            $this->getSponsorship = $this->pts->db->prepare(
                'SELECT * FROM ( SELECT * FROM pts WHERE r2 =  14 AND r4 = ? ORDER BY r0 DESC LIMIT 1 ) UNION
                                 SELECT * FROM pts WHERE r2 = -14 AND r4 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getSponsorship === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->getSponsorship->execute( [ $asset, $asset ] ) === false )
            w8_err( __FUNCTION__ );

        $pts = $this->getSponsorship->fetchAll();
        if( isset( $pts[0] ) && $pts[0][AMOUNT] !== 0 )
            $sponsorship = $pts[0];
        else
            $sponsorship = 0;

        $this->setSponsorship( $asset, $sponsorship );
        return $sponsorship;
    }

    private function setLeaseInfo( $id, $tx )
    {
        $this->leases->setKeyValue( $id, $tx );
    }

    private function getLeaseInfo( $id )
    {
        $tx = $this->leases->getValueByKey( $id );
        if( $tx === false )
        {
            if( false === ( $tx = wk()->fetch( '/leasing/info/' . $id, false, null, [ 404 ] ) ) )
                w8_err( __FUNCTION__ );

            if( null === ( $tx = wk()->json_decode( $tx ) ) )
                w8_err( __FUNCTION__ );
        }

        return $tx;
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
        $decimals = $tx['decimals'];
        $isNFT =
            true !== ( $tx['reissuable'] ?? $tx['isReissuable'] ) &&
            $tx['quantity'] === 1 &&
            $decimals === 0;
        $this->kvAssetInfo->setKeyValue( $id, ( $isNFT ? 'N' : $tx['decimals'] ) . chr( 0 ) . $name );
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
            w8_err( __FUNCTION__ . ': ' . $asset  );

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
        if( $txkey >= GetTxHeight_Sponsorship() )
        {
            $sponsorship = $this->getSponsorship( $afee );
            assert( $sponsorship !== 0 );

            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_SPONSOR,
                A =>        $this->getSenderId( $tx['sender'] ),
                B =>        $sponsorship[A],
                ASSET =>    $afee,
                AMOUNT =>   $tx['fee'],
                FEEASSET => WAVES_ASSET,
                FEE =>      gmp_intval( gmp_div( gmp_mul( $tx['fee'], 100000 ), $sponsorship[AMOUNT] ) ),
                ADDON =>    0,
                GROUP =>    0,
            ] );

            $tx[FEEASSET] = SPONSOR_ASSET;
            $tx[FEE] = 0;
        }
        else
        {
            $tx[FEEASSET] = $afee;
            $tx[FEE] = $tx['fee'];
        }
    }

    private function getPTS( $from, $to )
    {
        if( !isset( $this->q_getPTS ) )
        {
            $this->q_getPTS = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 >= ? AND r1 <= ?" );
            if( $this->q_getPTS === false )
                w8_err( __FUNCTION__ );
        }

        if( $this->q_getPTS->execute( [ $from, $to ] ) === false )
            w8_err( __FUNCTION__ );

        return $this->q_getPTS->fetchAll();
    }

    private function getFeesAt( $height, $reward )
    {
        $fees = [ WAVES_ASSET => $reward ];
        $ngfees = [];

        if( $this->workheight === $height )
            $pts = $this->feerecs;
        else
        {
            $this->flush();
            $pts = $this->getPTS( w8h2k( $height ), w8h2kg( $height ) - 1 );
        }

        foreach( $pts as $ts )
        {
            $fee = $ts[FEE];
            if( $fee === 0 || ( isset( $ts[TYPE] ) && $ts[TYPE] === TX_EXCHANGE ) ) // TX_EXCHANGE fees for MATCHER
                continue;

            $feeasset = $ts[FEEASSET];

            if( $height >= GetHeight_NG() )
            {
                $ngfee = intdiv( $fee, 5 ) * 2;
                $fees[$feeasset] = $ngfee + ( $fees[$feeasset] ?? 0 );
                $ngfees[$feeasset] = $fee - $ngfee + ( $ngfees[$feeasset] ?? 0 );
            }
            else
            {
                $fees[$feeasset] = $fee + ( $fees[$feeasset] ?? 0 );
            }
        }

        if( $height > GetHeight_NG() )
            foreach( $this->getNGFeesAt( $height - 1 ) as $feeasset => $fee )
                if( $fee > 0 )
                    $fees[$feeasset] = $fee + ( $fees[$feeasset] ?? 0 );

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

        if( $this->q_getNGFeesAt->execute( [ w8h2kg( $height ) ] ) === false )
            w8io_error( 'getNGFeesAt' );

        $pts = $this->q_getNGFeesAt->fetchAll();
        if( count( $pts ) < 1 )
            w8_err( "unexpected getNGFeesAt( $height )" );

        $ngfees = [];
        foreach( $pts as $ts )
            $ngfees[$ts[ASSET]] = $ts[ADDON];

        return $ngfees;
    }

    private function appendTS( $ts )
    {
        $this->recs[] = $ts;
        if( $ts[FEE] !== 0 && $ts[TYPE] !== TX_EXCHANGE ) // TX_EXCHANGE fees for MATCHER
            $this->feerecs[] = [ FEEASSET => $ts[FEEASSET], FEE => $ts[FEE] ];
    }

    private function processGeneratorTransaction( $txkey, $tx )
    {
        [ $fees, $ngfees ] = $this->getFeesAt( w8k2h( $txkey ), $tx['reward'] );

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
                FEEASSET => NO_ASSET,
                FEE =>      0,
                ADDON =>    $ngfees[$feeasset] ?? 0,
                GROUP =>    0,
            ];
        }

        $this->workheight = w8k2h( $txkey ) + 1;
        $this->feerecs = [];
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
                    ASSET =>    NO_ASSET,
                    AMOUNT =>   0,
                    FEEASSET => $tx[FEEASSET],
                    FEE =>      $tx[FEE],
                    ADDON =>    0,
                    GROUP =>    FAILED_GROUP,
                ] );
                break;
            case TX_EXCHANGE:
                // https://docs.waves.tech/en/blockchain/transaction/transaction-validation
                $fee = $tx['fee'];
                $afee = isset( $tx['feeAssetId'] ) ? $this->getAssetId( $tx['feeAssetId'] ) : WAVES_ASSET;
                $this->appendTS( [
                    UID =>      $this->getNewUid(),
                    TXKEY =>    $txkey,
                    TYPE =>     TX_MATCHER,
                    A =>        MATCHER,
                    B =>        $this->getRecipientId( $tx['sender'] ),
                    ASSET =>    $afee,
                    AMOUNT =>   -$fee,
                    FEEASSET => $afee,
                    FEE =>      $fee,
                    ADDON =>    0,
                    GROUP =>    FAILED_GROUP,
                ] );
                break;
            default:
                w8_err( 'processFailedTransaction unknown type: ' . $tx['type'] );
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
            FEEASSET => NO_ASSET,
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

    private function processIssueTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_ISSUE,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        MYSELF,
            ASSET =>    $this->getNewAssetId( $tx ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processInvokeIssueTransaction( $txkey, $tx, $dApp, $function )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_ISSUE,
            A =>        $dApp,
            B =>        MYSELF,
            ASSET =>    $this->getNewAssetId( $tx ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_ISSUE ),
        ] );
    }

    private function processReissueTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_REISSUE,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        MYSELF,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processInvokeReissueTransaction( $txkey, $tx, $dApp, $function )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_REISSUE,
            A =>        $dApp,
            B =>        MYSELF,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $tx['quantity'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_REISSUE ),
        ] );
    }

    private function processBurnTransaction( $txkey, $tx )
    {
        $amount = $tx['amount'] ?? $tx['quantity'];

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_BURN,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        MYSELF,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $amount,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processInvokeBurnTransaction( $txkey, $tx, $dApp, $function )
    {
        $amount = $tx['amount'] ?? $tx['quantity'];

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_BURN,
            A =>        $dApp,
            B =>        MYSELF,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   $amount,
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_BURN ),
        ] );
    }

    private function getQPrice( $asset )
    {
        if( $asset === 0 )
            $qp = 1;
        else
        switch( $this->kvAssetInfo->getValueByKey( $asset )[0] )
        {
            case 'N':
            case '0': $qp = 100000000; break;
            case '1': $qp = 10000000; break;
            case '2': $qp = 1000000; break;
            case '3': $qp = 100000; break;
            case '4': $qp = 10000; break;
            case '5': $qp = 1000; break;
            case '6': $qp = 100; break;
            case '7': $qp = 10; break;
            case '8': $qp = 1; break;
            default:
                w8io_error();
        }
        $qps = [ 100000000 * $qp, $qp ];
        $this->qps[$asset] = $qps;
        return $qps;
    }

    private function processExchangeTransaction( $txkey, $tx )
    {
        if( $tx['version'] >= 4 )
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
        $afee = isset( $tx['feeAssetId'] ) ? $this->getAssetId( $tx['feeAssetId'] ) : WAVES_ASSET;

        if( $buyer['version'] >= 4 )
            w8io_error();
        if( $seller['version'] >= 4 )
            w8io_error();

        // MATCHER;
        $diff = [];
        $diff[$bafee] = $bfee;
        $diff[$safee] = $sfee + ( $diff[$safee] ?? 0 );
        $diff[$afee] = -$fee + ( $diff[$afee] ?? 0 );
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
                    FEEASSET => NO_ASSET,
                    FEE =>      0,
                    ADDON =>    0,
                    GROUP =>    0,
                ] );
            }
        }

        if( $tx['version'] >= 3 )
        {
            $qa = $this->qps[$basset] ?? $this->getQPrice( $basset );
            $qp = $this->qps[$sasset] ?? $this->getQPrice( $sasset );

            $bamount = $tx['amount'];
            $price = $tx['price'];
            $samount = gmp_intval( gmp_div( gmp_mul( $price, $bamount ), intdiv( $qp[0], $qa[1] ) ) );
            $addon = intdiv( $price, $qp[1] );
        }
        else
        {
            $qa = $this->qps[$basset] ?? $this->getQPrice( $basset );

            $bamount = $tx['amount'];
            $price = $tx['price'];
            $samount = gmp_intval( gmp_div( gmp_mul( $price, $bamount ), 100000000 ) );
            $addon = intdiv( $price, $qa[1] );
        }

        

        // SELLER -> BUYER
        {
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_EXCHANGE,
                A =>        $sa,
                B =>        $ba,
                ASSET =>    $basset,
                AMOUNT =>   $bamount,
                FEEASSET => $safee,
                FEE =>      $sfee,
                ADDON =>    $addon,
                GROUP =>    $this->getGroupExchange( '>', $basset, $sasset ),
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
                AMOUNT =>   $samount,
                FEEASSET => $bafee,
                FEE =>      $bfee,
                ADDON =>    $addon,
                GROUP =>    $this->getGroupExchange( '<', $basset, $sasset ),
            ] );
        }
    }

    private function processTransferTransaction( $txkey, $tx )
    {
        $asset = $tx['assetId'];
        $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    $asset,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    0,
        ] );
    }

    private function processEthereumTransferTransaction( $txkey, $tx )
    {
        $asset = $tx['assetId'];
        $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;
        
        $sender = $this->getSenderId( $tx['sender'] );
        $recipient = $this->getRecipientId( $tx['recipient'] );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_ETHEREUM,
            A =>        $sender,
            B =>        $recipient,
            ASSET =>    NO_ASSET,
            AMOUNT =>   0,
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    ETHEREUM_TRANSFER_GROUP,
        ] );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $sender,
            B =>        $recipient,
            ASSET =>    $asset,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    0,
        ] );
    }

    private function processInvokeTransferTransaction( $txkey, $tx, $dApp, $function )
    {
        $asset = $tx['asset'];
        $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_TRANSFER,
            A =>        $dApp,
            B =>        $this->getRecipientId( $tx['address'] ),
            ASSET =>    $asset,
            AMOUNT =>   $tx['amount'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    $this->getAliasId( $tx['address'] ),
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_TRANSFER ),
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
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    0,
        ] );

        $this->setLeaseInfo( $tx['id'], $tx );
    }

    private function processInvokeLeaseTransaction( $txkey, $tx, $dApp, $function )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_LEASE,
            A =>        $dApp,
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    $this->getAliasId( $tx['recipient'] ),
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_LEASE ),
        ] );

        $this->setLeaseInfo( $tx['id'], $tx );
    }

    private function processLeaseCancelTransaction( $txkey, $tx )
    {
        $ltx = $this->getLeaseInfo( $tx['leaseId'] );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE_CANCEL,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $ltx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $ltx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processInvokeLeaseCancelTransaction( $txkey, $tx, $dApp, $function )
    {
        $ltx = $this->getLeaseInfo( $tx['id'] );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_LEASE_CANCEL,
            A =>        $dApp,
            B =>        $this->getRecipientId( $ltx['recipient'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $ltx['amount'],
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_LEASE_CANCEL ),
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
            B =>        MYSELF,
            ASSET =>    NO_ASSET,
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
        $asset = $tx['assetId'];
        $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;

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
                FEEASSET => NO_ASSET,
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
            B =>        MYSELF,
            ASSET =>    NO_ASSET,
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
            B =>        MYSELF,
            ASSET =>    NO_ASSET,
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
            B =>        MYSELF,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function getGroupExchange( $sb, $basset, $sasset )
    {
        $groupName = $sb . $basset . ':' . $sasset;
        return $this->kvGroups->getForcedKeyByValue( $groupName );
    }

    private function getGroupFunction( $dApp, $function, $type )
    {
        return $this->kvGroups->getForcedKeyByValue( $dApp . ':' . $function . ':' . $type );
    }

    private function processSponsorshipTransaction( $txkey, $tx )
    {
        $asset = $this->getAssetId( $tx['assetId'] );

        $ts = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_SPONSORSHIP,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $asset, // for serach by index tx + b
            ASSET =>    $asset,
            AMOUNT =>   $tx['minSponsoredAssetFee'] ?? 0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];

        $this->setSponsorship( $asset, $ts );
        $this->appendTS( $ts );
    }

    private function processInvokeSponsorshipTransaction( $txkey, $tx, $dApp, $function )
    {
        $asset = $this->getAssetId( $tx['assetId'] );

        $ts = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     ITX_SPONSORSHIP,
            A =>        $dApp,
            B =>        $asset, // for serach by index tx + b
            ASSET =>    $asset,
            AMOUNT =>   $tx['minSponsoredAssetFee'] ?? 0,
            FEEASSET => NO_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $dApp, $function, TX_SPONSORSHIP ),
        ];

        $this->setSponsorship( $asset, $ts );
        $this->appendTS( $ts );
    }

    private function significantStateChanges( $stateChanges )
    {
        if( count( $stateChanges['data'] ) !== 0 ) return true;
        if( count( $stateChanges['transfers'] ) !== 0 ) return true;
        if( count( $stateChanges['issues'] ) !== 0 ) return true;
        if( count( $stateChanges['reissues'] ) !== 0 ) return true;
        if( count( $stateChanges['burns'] ) !== 0 ) return true;
        if( count( $stateChanges['sponsorFees'] ) !== 0 ) return true;
        if( count( $stateChanges['leases'] ) !== 0 ) return true;
        if( count( $stateChanges['leaseCancels'] ) !== 0 ) return true;

        foreach( $stateChanges['invokes'] as $itx )
        {
            if( count( $itx['payment'] ) !== 0 ) return true;
            if( $this->significantStateChanges( $itx['stateChanges'] ) ) return true;
        }

        return false;
    }

    private function processInvokeTransaction( $txkey, $tx, $dAppToDapp = null, $ethereum = false )
    {
        $stateChanges = $tx['stateChanges'];
        $payments = $tx['payment'];
        $n = count( $payments );

        if( isset( $dAppToDapp ) )
        {
            if( $n !== 0 )
            {
                $payment = $payments[0];
                $asset = $payment['assetId'];
                $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;
                $amount = $payment['amount'];
            }
            else
            {
                $asset = NO_ASSET;
                $amount = 0;
                // if( !$this->significantStateChanges( $stateChanges ) ) return;
            }

            $sender = $dAppToDapp;
            $feeasset = 0;
            $fee = 0;
            $type = ITX_INVOKE;
        }
        else
        {
            if( $n !== 0 )
            {
                $payment = $payments[0];
                $asset = $payment['assetId'];
                $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;
                $amount = $payment['amount'];
            }
            else
            {
                $asset = NO_ASSET;
                $amount = 0;
            }

            $sender = $this->getSenderId( $tx['sender'], $tx );
            $feeasset = $tx[FEEASSET];
            $fee = $tx[FEE];
            $type = TX_INVOKE;
        }

        $dApp = $this->getRecipientId( $tx['dApp'] );
        $addon = $this->getAliasId( $tx['dApp'] );
        $function = $this->getFunctionId( $tx['call']['function'] ?? 'default' );
        $group = $this->getGroupFunction( $dApp, $function, TX_INVOKE );

        if( $ethereum )
        {
            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_ETHEREUM,
                A =>        $sender,
                B =>        $dApp,
                ASSET =>    NO_ASSET,
                AMOUNT =>   0,
                FEEASSET => NO_ASSET,
                FEE =>      0,
                ADDON =>    0,
                GROUP =>    $group,
            ] );
        }

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     $type,
            A =>        $sender,
            B =>        $dApp,
            ASSET =>    $asset,
            AMOUNT =>   $amount,
            FEEASSET => $feeasset,
            FEE =>      $fee,
            ADDON =>    $addon,
            GROUP =>    $group,
        ] );

        if( $n > 1 )
        for( $i = 1;; )
        {
            $payment = $payments[$i];
            $asset = $payment['assetId'];
            $asset = isset( $asset ) ? $this->getAssetId( $asset ) : WAVES_ASSET;
            $amount = $payment['amount'];

            $this->appendTS( [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     $type,
                A =>        $sender,
                B =>        $dApp,
                ASSET =>    $asset,
                AMOUNT =>   $amount,
                FEEASSET => NO_ASSET,
                FEE =>      0,
                ADDON =>    $addon,
                GROUP =>    $group,
            ] );

            if( ++$i >= $n )
                break;
        }

        return $this->processStateChanges( $txkey, $stateChanges, $dApp, $function );
    }

    private function processStateChanges( $txkey, $stateChanges, $dApp, $function )
    {
        if( $txkey >= GetTxHeight_RideV5() )
        {
            foreach( $stateChanges['invokes'] as $itx )
                $this->processInvokeTransaction( $txkey, $itx, $dApp );
            foreach( $stateChanges['issues'] as $itx )
                $this->processInvokeIssueTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['reissues'] as $itx )
                if( $itx['quantity'] !== 0 )
                    $this->processInvokeReissueTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['burns'] as $itx )
                if( $itx['quantity'] !== 0 )
                    $this->processInvokeBurnTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['sponsorFees'] as $itx )
                $this->processInvokeSponsorshipTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['transfers'] as $itx )
                if( $itx['amount'] !== 0 )
                    $this->processInvokeTransferTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['leaseCancels'] as $itx )
                    $this->processInvokeLeaseCancelTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['leases'] as $itx )
                $this->processInvokeLeaseTransaction( $txkey, $itx, $dApp, $function );
        }
        else
        if( $txkey >= GetTxHeight_RideV4() )
        {
            foreach( $stateChanges['issues'] as $itx )
                $this->processInvokeIssueTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['reissues'] as $itx )
                if( $itx['quantity'] !== 0 )
                    $this->processInvokeReissueTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['burns'] as $itx )
                if( $itx['quantity'] !== 0 )
                    $this->processInvokeBurnTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['sponsorFees'] as $itx )
                $this->processInvokeSponsorshipTransaction( $txkey, $itx, $dApp, $function );
            foreach( $stateChanges['transfers'] as $itx )
                if( $itx['amount'] !== 0 )
                    $this->processInvokeTransferTransaction( $txkey, $itx, $dApp, $function );
        }
        else
        {
            foreach( $stateChanges['transfers'] as $itx )
                if( $itx['amount'] !== 0 )
                    $this->processInvokeTransferTransaction( $txkey, $itx, $dApp, $function );
        }
    }

    private function processUpdateAssetInfoTransaction( $txkey, $tx )
    {
        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_UPDATE_ASSET_INFO,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        MYSELF,
            ASSET =>    $this->getUpdatedAssetId( $tx ),
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ] );
    }

    private function processEthereumTransaction( $txkey, $tx )
    {
        $payload = $tx['payload'];
        switch( $payload['type'] )
        {
            case 'transfer':
                $tx['recipient'] = $payload['recipient'];
                $tx['assetId'] = $payload['asset'];
                $tx['amount'] = $payload['amount'];
                return $this->processEthereumTransferTransaction( $txkey, $tx );

            case 'invocation':
                $tx['dApp'] = $payload['dApp'];
                $tx['call'] = $payload['call'];
                $tx['payment'] = $payload['payment'];
                $tx['stateChanges'] = $payload['stateChanges'];
                return $this->processInvokeTransaction( $txkey, $tx, null, true );

            default:
                w8io_error( 'unknown payload type: ' . $payload['type'] );
        }
    }

    private function processExpressionTransaction( $txkey, $tx )
    {
        $sender = $this->getSenderId( $tx['sender'] );

        $this->appendTS( [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_EXPRESSION,
            A =>        $sender,
            B =>        MYSELF,
            ASSET =>    NO_ASSET,
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    $this->getGroupFunction( $sender, EXPRESSION_FUNCTION, TX_EXPRESSION ),
        ] );

        return $this->processStateChanges( $txkey, $tx['stateChanges'], $sender, EXPRESSION_FUNCTION );
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
                    w8_err( 'applicationStatus unknown: ' . $tx['applicationStatus'] );
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
            case TX_EXPRESSION:
                $this->processExpressionTransaction( $txkey, $tx ); break;
            case TX_ETHEREUM:
                $this->processEthereumTransaction( $txkey, $tx ); break;

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
        $this->leases->reset();
        $this->setHighs();
        $this->feerecs = [];
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

