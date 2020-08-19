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
    public KV $kvAddons;
    public KV $kvAssets;
    public KV $kvAssetNames;
    public KV $kvAssetDecimals;
    public KV $kvSponsors;
    public Blockchain $blockchain;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->pts = new Triples( $this->db , 'pts', 1,
            // uid                 | txkey    | type     | a        | b        | asset    | amount   | feeasset | fee      | addon    | group
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7       | r8       | r9       | r10
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         1,         1,         1,         1,         0,         1,         0,         0,         1 ] );

        $this->kvAddresses =     ( new KV( true )  )->setStorage( $this->db, 'addresses', true );
        $this->kvAliases =       ( new KV( false ) )->setStorage( $this->db, 'aliases', true, 'TEXT UNIQUE', 'INTEGER' );
        $this->kvAddons =        ( new KV( true )  )->setStorage( $this->db, 'addons', true );
        $this->kvAssets =        ( new KV( true )  )->setStorage( $this->db, 'assets', true );
        $this->kvAssetNames =    ( new KV( false ) )->setStorage( $this->db, 'assetNames', true, 'INTEGER PRIMARY KEY', 'TEXT' );
        $this->kvAssetDecimals = ( new KV( false ) )->setStorage( $this->db, 'assetDecimals', true, 'INTEGER PRIMARY KEY', 'INTEGER' );
        
        $this->sponsorships = new KV;

        $this->decimals = [];

        $this->kvs = [
            $this->kvAddresses,
            $this->kvAliases,
            $this->kvAddons,
            $this->kvAssets,
            $this->kvAssetNames,
            $this->kvAssetDecimals,
        ];
        
        $predefinedAddresses = [
            GENESIS => 'GENESIS',
            GENERATOR => 'GENERATOR',
            MATCHER => 'MATCHER',
            UNDEFINED => 'UNDEFINED',
            SPONSOR => 'SPONSOR',
            MASS => 'MASS',
        ];

        foreach( $predefinedAddresses as $k => $v )
            if( $v !== $this->kvAddresses->getValueByKey( $k ) )
                $this->kvAddresses->setKeyValue( $k, $v );
        $this->kvAddresses->merge();
        $this->kvAddresses->setHigh();

        $predefinedAssets = [
            WAVES_ASSET => 'Waves',
        ];

        foreach( $predefinedAssets as $k => $v )
            if( $v !== $this->kvAssets->getValueByKey( $k ) )
                $this->kvAssets->setKeyValue( $k, $v );
        $this->kvAssets->merge();
        $this->kvAssets->setHigh();

        $blobs = [ 'GENESIS', 'GENERATOR', 'MATCHER', 'NULL', 'SPONSOR', 'MASS', 'HUYAS', '123', '12938' ];

        //$ids = $this->getIdsByBlobs( $this->dbAddresses, $blobs );
        //$ids = $this->setIdsByBlobs( $this->dbAddresses, $blobs );
        //$ids = $this->setIdsByBlobs( $this->dbAddresses, $blobs );

        $this->setHighs();
        $this->recs = [];
    }

    private function setSponsorship( $asset, $sponsorship )
    {
        $this->sponsorships->setKeyValue( $asset, $sponsorship );
    }

    private function getSponsorship( $asset )
    {
        $sponsorship = $this->sponsorships->getValueByKey( $asset );
        if( $sponsorship === false )
        {
            $sponsorship = 0;
            $ts = $this->pts->query( 'SELECT * FROM pts WHERE r'. TYPE . ' = ' . TX_SPONSORSHIP . ' AND r' . ASSET . ' = ' . $asset . ' ORDER BY r0 ASC LIMIT 1' )->fetchAll();
            if( isset( $ts[0] ) && $ts[0][AMOUNT] !== '0' )
                $sponsorship = $ts[0];

            $this->setSponsorship( $asset, $sponsorship );
        }

        return $sponsorship === 0 ? false : $sponsorship;
    }

    private function getLeaseInfoById( $id )
    {
        $txkey = $this->blockchain->getTransactionKey( $id );
        if( $txkey === false )
            w8_err( 'getLeaseInfoById > getTransactionKey' );

        foreach( $this->recs as $ts )
            if( $ts[TXKEY] === $txkey && $ts[TYPE] === TX_LEASE )
                return $ts;

        static $q;
        if( !isset( $q ) )
        {
            $q = $this->pts->db->prepare( 'SELECT * FROM pts WHERE r1 == ? AND r2 == ' . TX_LEASE );
            if( $q === false )
                w8io_error( "getLeaseInfoById" );
        }

        if( $q->execute( [ $txkey ] ) === false )
            w8io_error( "getLeaseInfoById( $id )" );

        $ts = $q->fetchAll();
        if( $ts === false )
            w8io_error( "getLeaseInfoById( $id )" );

        return $ts[0];
    }

    private function getTS( $key )
    {
        return $this->pts->getUno( 1, $key );
    }

    private function setHighs()
    {
        $this->setUid();
    }

    public function setBlockchain( $blockchain )
    {
        $this->blockchain = $blockchain;
    }

    public function getIdsByBlobs( Triples $db, $blobs )
    {
        $n = count( $blobs );

        $query = 'SELECT * FROM ' . $db->name() . ' WHERE r2 IN ( ' . ( $n > 1 ? str_repeat( '?,', $n - 1 ) : '' ) . '? )';
        if( false === ( $query = $db->query( $query, $blobs ) ) )
            return false;

        $ids = [];
        foreach( $query as $r )
            $ids[$r[1]] = (int)$r[0];

        return $ids;
    }

    public function getBlobsByIds( $db, $ids )
    {
        $n = count( $ids );

        $query = 'SELECT * FROM ' . $db->name() . ' WHERE r1 IN ( ' . ( $n > 1 ? str_repeat( '?,', $n - 1 ) : '' ) . '? )';
        if( false === ( $query = $db->query( $query, $ids ) ) )
            return false;

        $blobs = [];
        foreach( $query as $r )
            $blobs[$r[0]] = $r[1];

        return $blobs;
    }

    public function setIdsByBlobs( $db, $blobs )
    {
        $ids = $this->getIdsByBlobs( $db, $blobs );

        $n = count( $blobs );
        if( $n === count( $ids ) )
            return $ids;

        $hi = $db->getHi( 1 );

        $set = [];
        for( $i = 0; $i < $n; $i++ )
        {
            $blob = $blobs[$i];
            if( !isset( $ids[$blob] ) )
            {
                $id = ++$hi;                
                $ids[$blob] = $id;
                $set[] = [ $id, $blob ];
            }
        }

        $db->merge( $set );
        return $ids;
    }

    private function get_txid( $txid, $one = false )
    {
        if( !isset( $this->query_get_txid ) )
        {
            $this->query_get_txid = $this->transactions->prepare( 'SELECT * FROM transactions WHERE txid = :txid ORDER BY uid ASC' );
            if( !is_object( $this->query_get_txid ) )
                return false;
        }

        if( $this->query_get_txid->execute( [ 'txid' => $txid ] ) === false )
            return false;

        $data = $this->query_get_txid->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $data[0] ) )
            return false;

        if( $one )
        {
            if( count( $data ) !== 1 )
                return false;

            return w8io_filter_wtx( $data[0] );
        }

        return array_map( 'w8io_filter_wtx', $data );
    }

    private function get_wtxs_at( $at )
    {
        if( !isset( $this->query_wtxs_at ) )
        {
            $this->query_wtxs_at = $this->transactions->prepare( "SELECT * FROM transactions WHERE block = :at" );
            if( !is_object( $this->query_wtxs_at ) )
                return false;
        }

        if( $this->query_wtxs_at->execute( [ 'at' => $at ] ) === false )
            return false;

        return array_map( 'w8io_filter_wtx', $this->query_wtxs_at->fetchAll( PDO::FETCH_ASSOC ) );
    }

    public function get_height()
    {
        $height = $this->checkpoint->getValue( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, 'i' );
        if( !$height )
            return 0;

        return $height;
    }

    private function clear_transactions( $height )
    {
        if( !isset( $this->query_clear ) )
        {
            $this->query_clear = $this->transactions->prepare( 'DELETE FROM transactions WHERE block > :height' );
            if( !is_object( $this->query_clear ) )
                return false;
        }

        if( $this->query_clear->execute( [ 'height' => $height ] ) === false )
            return false;

        return true;
    }

    private function timestamp( $timestamp )
    {
        return intdiv( $timestamp, 1000 );
    }

    private function get_pair_txid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_txids->getKey( $id, $new ) ) )
            w8io_error();
        return $id;
    }

    public function get_txid_by_id( $id )
    {
        if( !isset( $this->pairs_txids ) )
            $this->pairs_txids = new Pairs( $this->transactions, 'txids' );

        if( false === ( $id = $this->pairs_txids->getValue( $id, 's' ) ) )
            w8io_error();

        return $id;
    }

    private function get_assetid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_assets->getKey( $id, $new ) ) )
            w8io_error( $id );
        return $id;
    }

    private function get_dataid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_addons->getKey( $id, $new ) ) )
            w8io_error();
        return $id;
    }

    private function get_aid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_addresses->getKey( $id, $new ) ) )
            w8io_error();
        return $id;
    }

    public function get_txs_all( $limit = 100 )
    {
        if( !isset( $this->query_get_txs_all ) )
        {
            $this->query_get_txs_all = $this->transactions->prepare(
                "SELECT * FROM transactions ORDER BY uid DESC LIMIT :limit" );
            if( !is_object( $this->query_get_txs_all ) )
                return false;
        }

        if( $this->query_get_txs_all->execute( [ 'limit' => $limit ] ) === false )
            return false;

        return $this->query_get_txs_all;
    }

    public function query( $query )
    {
        $query = $this->transactions->prepare( $query );
        if( !is_object( $query ) )
            return false;

        if( $query->execute() === false )
            return false;

        return $query;
    }

    public function get_txs_where( $aid, $where, $uid = false, $limit = 100 )
    {
        if( $aid !== false )
        {
            $where = $where ? ' AND ' . $where : '';
            if( $uid )
                $where .= ' AND uid <= ' . $uid;
            $where =
                "SELECT * FROM ( SELECT * FROM transactions WHERE a = $aid$where ORDER BY uid DESC LIMIT $limit ) UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE b = $aid$where ORDER BY uid DESC LIMIT $limit ) ORDER BY uid DESC LIMIT $limit";
        }
        else
        {
            $where = $where ? ' WHERE ' . $where : '';
            if( $uid )
                $where .= ( strlen( $where ) ? ' AND ' : ' WHERE ' ) . 'uid <= ' . $uid;
            $where =
                "SELECT * FROM transactions$where ORDER BY uid DESC LIMIT $limit";
        }

        $query_where = $this->transactions->prepare( $where );
        if( !is_object( $query_where ) )
            return false;

        if( $query_where->execute() === false )
            return false;

        return $query_where;
    }

    public function get_txs( $aid, $height, $limit = 100 )
    {
        if( !isset( $this->query_get_txs ) )
        {
            $this->query_get_txs = $this->transactions->prepare(
                "SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND a = :aid ORDER BY uid DESC LIMIT :limit )
                 UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND b = :aid ORDER BY uid DESC LIMIT :limit ) ORDER BY uid DESC LIMIT :limit" );
            if( !is_object( $this->query_get_txs ) )
                return false;
        }

        if( $this->query_get_txs->execute( [ 'aid' => $aid, 'height' => $height, 'limit' => $limit ] ) === false )
            return false;

        return $this->query_get_txs;
    }

    public function get_txs_asset( $aid, $height, $asset, $limit = 100 )
    {
        if( !isset( $this->query_get_txs_asset ) )
        {
            $this->query_get_txs_asset = $this->transactions->prepare(
                "SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND a = :aid AND asset = :asset ORDER BY uid DESC LIMIT :limit )
                 UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND b = :aid AND asset = :asset ORDER BY uid DESC LIMIT :limit ) ORDER BY uid DESC LIMIT :limit" );
            if( !is_object( $this->query_get_txs_asset ) )
                return false;
        }

        if( $this->query_get_txs_asset->execute( [ 'aid' => $aid, 'height' => $height, 'asset' => $asset, 'limit' => $limit ] ) === false )
            return false;

        return $this->query_get_txs_asset;
    }

    public function get_from_to( $from, $to )
    {
        if( !isset( $this->query_from_to ) )
        {
            $this->query_from_to = $this->transactions->prepare( "SELECT * FROM transactions WHERE block > :from AND block <= :to ORDER BY uid ASC" );
            if( !is_object( $this->query_from_to ) )
                return false;
        }

        if( $this->query_from_to->execute( [ 'from' => $from, 'to' => $to ] ) === false )
            return false;

        return array_map( 'w8io_filter_wtx', $this->query_from_to->fetchAll( PDO::FETCH_ASSOC ) );
    }

    public function mark_scam( $scam, $mark )
    {
        $id = $this->pairs_assets->getKey( $scam );
        if( $id === false )
            return;

        $info = $this->pairs_asset_info->getValue( $id, 'j' );
        if( $info === false )
            return;

        if( $mark )
        {
            if( isset( $info['scam'] ) )
                return;

            $info['scam'] = 1;
        }
        else
        {
            if( !isset( $info['scam'] ) )
                return;

            unset( $info['scam'] );
        }

        $this->pairs_asset_info->setKeyValue( $id, $info, 'j' );
    }

    public function mark_tickers( $ticker, $mark )
    {
        $id = $this->pairs_assets->getKey( $ticker );
        if( $id === false )
            return;

        $info = $this->pairs_asset_info->getValue( $id, 'j' );
        if( $info === false )
            return;

        if( $mark )
        {
            if( isset( $info['ticker'] ) )
                return;

            $info['ticker'] = 1;
        }
        else
        {
            if( !isset( $info['ticker'] ) )
                return;

            unset( $info['ticker'] );
        }

        $this->pairs_asset_info->setKeyValue( $id, $info, 'j' );
    }

    private function sponsor_swap( $wtx, $sponsor, $wfee )
    {
        // A > SPONSOR
        $wtx['b'] = $sponsor;
        $wtx['amount'] = $wtx['fee'];
        $wtx['asset'] = $wtx['afee'];

        $wtx['type'] = W8IO_TYPE_SPONSOR;
        $wtx['fee'] = 0;
        $wtx['data'] = false;

        if( !$this->set_tx( $wtx ) )
            w8io_error( json_encode( $wtx ) );

        // SPONSOR > METASPONSOR
        $wtx['a'] = $sponsor;
        $wtx['b'] = 'SPONSOR';
        $wtx['amount'] = $wfee;
        $wtx['asset'] = 0;

        if( !$this->set_tx( $wtx ) )
            w8io_error( json_encode( $wtx ) );
    }

    private function fill_sponsors()
    {
        $query_sponsorships = $this->transactions->prepare( "SELECT * FROM transactions WHERE type = 14 ORDER BY uid DESC" );
        if( !is_object( $query_sponsorships ) )
            w8io_error( 'query_sponsorships->prepare()' );

        if( $query_sponsorships->execute() === false )
            w8io_error( 'query_sponsorships->execute()' );

        $this->sponsors = [];

        foreach( $query_sponsorships as $wtx )
            if( !isset( $this->sponsors[$wtx['asset']] ) )
                $this->set_sponsor( (int)$wtx['asset'], (int)$wtx['a'], (int)$wtx['amount'] );
    }

    private function set_sponsor( $asset, $a, $sfee )
    {
        if( !isset( $this->sponsors ) )
            $this->sponsors = [];

        $this->sponsors[$asset] = [ 'a' => $a, 'f' => $sfee ];
    }

    private function get_sponsor( $asset )
    {
        if( !isset( $this->sponsors[$asset] ) )
            return false;

        return $this->sponsors[$asset];
    }

    private function set_tx( $wtx )
    {
        $a = $wtx['a'];
        if( is_int( $a ) === false )
        {
            if( $a[0] === 'a' )
            {
                w8io_error( json_encode( $wtx ) );
            }
            else if( $a[0] === '3' )
            {
                $a = $this->pairs_addresses->getKey( $wtx['a'], true );
                if( $a === false )
                    return false;
            }
            else if( $a === 'GENESIS' )
            {
                $a = 0;
            }
            else if( $a === 'GENERATOR' )
            {
                $a = -1;
            }
            else if( $a === 'MATCHER' )
            {
                $a = -2;
            }
            else
                w8io_error( json_encode( $wtx ) );
        }

        $b = $wtx['b'];
        if( is_int( $b ) === false )
        {
            if( $b === false )
            {
                $b = 0;
            }
            else if( $b[0] === 'a' )
            {
                $alias = substr( $b, 8 );

                $b = $this->pairs_aliases->getValue( $alias, 'i' );
                if( $b === false )
                    return false;

                if( $wtx['data'] !== false )
                    $wtx['data']['b'] = $this->get_dataid( $alias );
                else
                    $wtx['data'] = [ 'b' => $this->get_dataid( $alias ) ];
            }
            else if( $b[0] === '3' )
            {
                $b = $this->pairs_addresses->getKey( $wtx['b'], true );
                if( $b === false )
                    return false;
            }
            else if( $b === 'NULL' )
            {
                $b = -3;
            }
            else if( $b === 'SPONSOR' )
            {
                $b = -4;
            }
            else if( $b === 'MASS' )
            {
                $b = -5;
            }
            else
                w8io_error( json_encode( $wtx ) );
        }

        if( !isset( $this->query_set_tx ) )
        {
            $this->query_set_tx = $this->transactions->prepare( "INSERT INTO transactions
                (  txid,  block,  type,  timestamp,  a,  b,  amount,  asset,  fee,  afee,  data ) VALUES
                ( :txid, :block, :type, :timestamp, :a, :b, :amount, :asset, :fee, :afee, :data )" );
            if( !is_object( $this->query_set_tx ) )
                return false;
        }

        $wtx['a'] = $a;
        $wtx['b'] = $b;

        if( $wtx['fee'] === 0 )
            $wtx['afee'] = W8IO_ASSET_EMPTY;
        else if( $wtx['afee'] > 0 && $wtx['block'] >= W8IO_SPONSOR_ACTIVE && ( $sponsor = $this->get_sponsor( $wtx['afee'] ) ) )
        {
            $this->sponsor_swap( $wtx, $sponsor['a'], gmp_intval( gmp_div( gmp_mul( $wtx['fee'], 100000 ), $sponsor['f'] ) ) );

            if( $wtx['data'] !== false )
                $wtx['data']['f'] = $sponsor['f'];
            else
                $wtx['data'] = [ 'f' => $sponsor['f'] ];
        }

        if( $wtx['data'] !== false )
            $wtx['data'] = json_encode( $wtx['data'] );

        if( $this->query_set_tx->execute( $wtx ) === false )
            return false;

        $this->wtxs[] = $wtx;
        return true;
    }

    private function block_fees( $at, $wtxs, $prev_wtxs )
    {
        $fees = [ 0 => 0 ];
        $prev = false;

        for( ;; )
        {
            foreach( $wtxs as $wtx )
            {
                $fee = $wtx['fee'];
                if( $fee === 0 )
                    continue;

                // only MATCHER pays fee to GENERATOR
                if( $wtx['type'] === 7 && $wtx['a'] !== -2 )
                    continue;

                $asset = $wtx['afee'];

                if( $at >= W8IO_NG_ACTIVE )
                {
                    if( $asset > 0 && $at >= W8IO_SPONSOR_ACTIVE && $wtx['data'] !== false )
                    {
                        $data = json_decode( $wtx['data'], true );
                        if( isset( $data['f'] ) )
                        {
                            $asset = 0;
                            $fee = gmp_intval( gmp_div( gmp_mul( $fee, 100000 ), $data['f'] ) );
                        }
                    }

                    $ngfee = intdiv( $fee, 5 ) * 2;
                    $fee = $prev ? $fee - $ngfee : $ngfee;
                    if( $fee === 0 )
                        continue;
                }

                $fees[$asset] = isset( $fees[$asset] ) ? $fees[$asset] + $fee : $fee;
            }

            if( $prev || $at <= W8IO_NG_ACTIVE )
                return $fees;

            $wtxs = $prev_wtxs;
            $prev = true;
        }
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

    private function getSenderId( $address )
    {
        $id = $this->kvAddresses->getKeyByValue( $address );
        if( $id === false )
            w8io_error( 'getSenderId' );
        
        return $id;
    }

    private function getRecipientId( $addressOrAlias )
    {
        if( $addressOrAlias[0] === '3' && strlen( $addressOrAlias ) === 35 )
            return $this->kvAddresses->getForcedKeyByValue( $addressOrAlias );

        if( substr( $addressOrAlias, 0, 6 ) !== 'alias:')
            w8io_error( 'unexpected $addressOrAlias = ' . $addressOrAlias );
        
        $id = $this->kvAliases->getValueByKey( substr( $addressOrAlias, 8 ) );
        if( $id === false )
            w8io_error( 'getRecipientId' );
        
        return $id;
    }

    private function getAliasId( $alias )
    {
        if( substr( $alias, 0, 6 ) !== 'alias:')
            w8io_error( 'unexpected $alias = ' . $alias );
        
        $id = $this->kvAliases->getValueByKey( substr( $alias, 8 ) );
        if( $id === false )
            w8io_error( 'getAliasId' );
        
        return $id;
    }

    private function getNewAssetId( $tx )
    {
        $id = $this->kvAssets->getForcedKeyByValue( $tx['assetId'] );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $this->kvAssetNames->setKeyValue( $id, $name );
        $this->kvAssetDecimals->setKeyValue( $id, $tx['decimals'] );
        return $id;
    }

    private function getUpdatedAssetId( $tx )
    {
        $id = $this->kvAssets->getKeyByValue( $tx['assetId'] );
        if( $id === false )
            w8io_error( 'getUpdatedAssetId' );
        $name = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
        $this->kvAssetNames->setKeyValue( $id, $name );
        return $id;
    }

    private function getAssetId( $asset )
    {
        $id = $this->kvAssets->getKeyByValue( $asset );
        if( $id === false )
            w8io_error( 'getAssetId' );
        
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
        if( $sponsorship !== false )
        {
            $this->recs[] = [
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
            ];

            $tx[FEEASSET] = SPONSOR_ASSET;
            $tx[FEE] = 0;
            return;
        }

        $tx[FEEASSET] = $afee;
        $tx[FEE] = $tx['fee'];
    }

    private function getAssetInfo( $assetInt )
    {
        $info = $this->kvAssetInfo->getValueByKey( $assetInt );
        if( $info === false )
            w8io_error( 'getAssetDecimals' );

        return $info;
    }

    private function getAssetDecimals( $assetInt )
    {
        if( !isset( $this->decimals[$assetInt] ) )
            $this->decimals[$assetInt] = wk()->json_decode( $this->getAssetInfo( $assetInt ) )['decimals'];

        return $this->decimals[$assetInt];        
    }

    private function getPTS( $height )
    {
        static $q;

        if( !isset( $q ) )
        {
            $q = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 >= ? AND r1 < ?" );
            if( $q === false )
                w8io_error( 'getPTS' );
        }

        if( $q->execute( [ w8_hi2k( $height ), w8_hi2k( $height + 1 ) - 1 ] ) === false )
            w8io_error( 'getPTS' );

        return $q->fetchAll();
    }
/*
    define( 'UID', 0 );
define( 'TXKEY', 1 );
define( 'TYPE', 2 );
define( 'A', 3 );
define( 'B', 4 );
define( 'ASSET', 5 );
define( 'AMOUNT', 6 );
define( 'FEEASSET', 7 );
define( 'FEE', 8 );
define( 'ADDON', 9 );
define( 'GROUP', 10 );
*/
    private function getFeesAt( $height, $reward )
    {
        $fees = [ WAVES_ASSET => $reward ];
        $ngfees = [];

        foreach( $this->getPTS( $height ) as $ts )
        {
            $fee = (int)$ts[FEE];
            if( $fee <= 0 )
                continue;

            if( (int)$ts[TYPE] === TX_EXCHANGE ) // TX_MATCHER pays real fees
                continue;

            $feeasset = (int)$ts[FEEASSET];

            if( $height >= NGHeight() )
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

        if( $height > NGHeight() )
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

        static $q;

        if( !isset( $q ) )
        {
            $q = $this->pts->db->prepare( "SELECT * FROM pts WHERE r1 == ?" );
            if( $q === false )
                w8io_error( 'getNGFeesAt' );
        }

        if( $q->execute( [ w8_hi2k( $height + 1 ) - 1 ] ) === false )
            w8io_error( 'getNGFeesAt' );

        $pts = $q->fetchAll();
        if( count( $pts ) < 1 )
            w8_err( "unexpected getNGFeesAt( $height )" );

        $ngfees = [];
        foreach( $pts as $ts )
            $ngfees[(int)$ts[ASSET]] = (int)$ts[ADDON];
    
        return $ngfees;
    }

    private function processGeneratorTransaction( $txkey, $tx )
    {
        $this->flush();

        list( $fees, $ngfees ) = $this->getFeesAt( w8_k2h( $txkey ), $tx['reward'] );

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
    }

    private function processFailedTransaction( $txkey, $tx )
    {
        switch( $tx['type'] )
        {
            case TX_INVOKE:
                $this->recs[] = [
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
                ];
                break;
            default:
                w8_err( 'unknown failed transaction ' . $tx['type'] );
        }
    }

    private function processGenesisTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
    }

    private function processPaymentTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
    }

    private function processIssueTransaction( $txkey, $tx, $dApp = null )
    {
        $this->recs[] = [
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
        ];
    }

    private function processReissueTransaction( $txkey, $tx, $dApp = null )
    {
        $this->recs[] = [
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
        ];
    }

    private function processBurnTransaction( $txkey, $tx, $dApp = null )
    {
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_BURN,
            A =>        isset( $dApp ) ? $dApp : $this->getSenderId( $tx['sender'] ),
            B =>        UNDEFINED,
            ASSET =>    $this->getAssetId( $tx['assetId'] ),
            AMOUNT =>   isset( $dApp ) ? $tx['quantity'] : $tx['amount'],
            FEEASSET => isset( $dApp ) ? INVOKE_ASSET : $tx[FEEASSET],
            FEE =>      isset( $dApp ) ? 0 : $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];
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

        if( 0 )
        {
            $bname = $info['name'];
            $bdecimals = $info['decimals'];
            $sname = $info['name'];
            $sdecimals = $info['decimals'];
        }

        $bfee = $tx['buyMatcherFee'];
        $sfee = $tx['sellMatcherFee'];
        $fee = $tx['fee'];
        $bafee = isset( $buyer['matcherFeeAssetId'] ) ? $this->getAssetId( $buyer['matcherFeeAssetId'] ) : WAVES_ASSET;
        $safee = isset( $seller['matcherFeeAssetId'] ) ? $this->getAssetId( $seller['matcherFeeAssetId'] ) : WAVES_ASSET;
        $afee = isset( $tx['feeAssetId'] ) ? $this->getAssetId( $tx['feeAssetId'] ) : 0;

        if( $bafee && false !== $this->getSponsorship( $bafee ) )
            w8io_error();
        if( $safee && false !== $this->getSponsorship( $safee ) )
            w8io_error();
        
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
                $this->recs[] = [
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
                ];
            }
            else if( $mamount )
            {
                $this->recs[] = [
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
                ];
            }
        }

        // price
        if( 0 )
        {
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
        }

        $amount = $tx['amount'];

        // SELLER -> BUYER
        {
            $this->recs[] = [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_EXCHANGE,
                A =>        $sa,
                B =>        $ba,
                ASSET =>    $basset,
                AMOUNT =>   $amount,
                FEEASSET => $safee,
                FEE =>      $sfee,
                ADDON =>    0,
                GROUP =>    0,
            ];
        }
        // BUYER -> SELLER
        {
            $this->recs[] = [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_EXCHANGE,
                A =>        $ba,
                B =>        $sa,
                ASSET =>    $sasset,
                AMOUNT =>   gmp_intval( gmp_div( gmp_mul( $tx['price'], $amount ), 100000000 ) ),
                FEEASSET => $bafee,
                FEE =>      $bfee,
                ADDON =>    0,
                GROUP =>    0,
            ];
        }
    }

    private function processTransferTransaction( $txkey, $tx, $dApp = null )
    {
        if( isset( $dApp ) )
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $dApp,
            B =>        $this->getRecipientId( $tx['address'] ),
            ASSET =>    isset( $tx['asset'] ) ? $this->getAssetId( $tx['asset'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => INVOKE_ASSET,
            FEE =>      0,
            ADDON =>    $tx['address'][0] === 'a' ? $this->getAliasId( $tx['address'] ) : 0,
            GROUP =>    0,
        ];
        else
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_TRANSFER,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    $tx['recipient'][0] === 'a' ? $this->getAliasId( $tx['recipient'] ) : 0,
            GROUP =>    0,
        ];
    }

    private function processLeaseTransaction( $txkey, $tx )
    {
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        $this->getRecipientId( $tx['recipient'] ),
            ASSET =>    isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET,
            AMOUNT =>   $tx['amount'],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processLeaseCancelTransaction( $txkey, $tx )
    {
        //$this->flush();
        $ts = $this->getLeaseInfoById( $tx['leaseId'] );
        if( $ts === false )
            w8io_error();

        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE_CANCEL,
            A =>        $this->getSenderId( $tx['sender'] ),
            B =>        (int)$ts[B],
            ASSET =>    (int)$ts[ASSET],
            AMOUNT =>   (int)$ts[AMOUNT],
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processAliasTransaction( $txkey, $tx )
    {
        $a = $this->getSenderId( $tx['sender'] );
        $this->kvAliases->setKeyValue( $tx['alias'], $a );

        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_LEASE,
            A =>        $a,
            B =>        UNDEFINED,
            ASSET =>    0,
            AMOUNT =>   0,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processMassTransferTransaction( $txkey, $tx )
    {
        $a = $this->getSenderId( $tx['sender'] );
        $asset = isset( $tx['assetId'] ) ? $this->getAssetId( $tx['assetId'] ) : WAVES_ASSET;

        $this->recs[] = [
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
        ];

        foreach( $tx['transfers'] as $mtx )
            $this->recs[] = [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_MASS_TRANSFER,
                A =>        $a,
                B =>        $this->getRecipientId( $mtx['recipient'] ),
                ASSET =>    $asset,
                AMOUNT =>   $mtx['amount'],
                FEEASSET => 0,
                FEE =>      0,
                ADDON =>    0,
                GROUP =>    0,
            ];
    }

    private function processDataTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
    }

    private function processSmartAccountTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
    }

    private function processSmartAssetTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
    }

    private function processSponsorshipTransaction( $txkey, $tx, $dApp = null )
    {
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
            GROUP =>    0,
        ];

        $this->setSponsorship( $ts[ASSET], $ts );
        $this->recs[] = $ts;
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

        $sender = $this->getSenderId( $tx['sender'] );
        $dApp = $this->getRecipientId( $tx['dApp'] );

        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_INVOKE,
            A =>        $sender,
            B =>        $dApp,
            ASSET =>    $asset,
            AMOUNT =>   $amount,
            FEEASSET => $tx[FEEASSET],
            FEE =>      $tx[FEE],
            ADDON =>    0,
            GROUP =>    0,
        ];

        if( isset( $tx['payment'][1] ) )
        {
            $payment = $tx['payment'][1];
            $asset = isset( $payment['assetId'] ) ? $this->getAssetId( $payment['assetId'] ) : WAVES_ASSET;
            $amount = $payment['amount'];

            $this->recs[] = [
                UID =>      $this->getNewUid(),
                TXKEY =>    $txkey,
                TYPE =>     TX_INVOKE,
                A =>        $sender,
                B =>        $dApp,
                ASSET =>    $asset,
                AMOUNT =>   $amount,
                FEEASSET => 0,
                FEE =>      0,
                ADDON =>    0,
                GROUP =>    0,
            ];
        }

        if( isset( $tx['payment'][2] ) )
            w8_err( 'unexpected 3rd payment' );

        $stateChanges = $tx['stateChanges'];

        //foreach( $stateChanges['data'] as $itx )
            //$this->processDataTransaction( $txkey, $itx, $dApp );
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

    private function processUpdateAssetInfoTransaction( $txkey, $tx )
    {
        $this->recs[] = [
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
        ];
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

        switch( $type )
        {
            case TX_PAYMENT:
                return $this->processPaymentTransaction( $txkey, $tx );
            case TX_ISSUE:
                return $this->processIssueTransaction( $txkey, $tx );
            case TX_TRANSFER:
                return $this->processTransferTransaction( $txkey, $tx );
            case TX_REISSUE:
                return $this->processReissueTransaction( $txkey, $tx );
            case TX_BURN:
                return $this->processBurnTransaction( $txkey, $tx );
            case TX_EXCHANGE:
                return $this->processExchangeTransaction( $txkey, $tx );
            case TX_LEASE:
                return $this->processLeaseTransaction( $txkey, $tx );
            case TX_LEASE_CANCEL:
                return $this->processLeaseCancelTransaction( $txkey, $tx );
            case TX_ALIAS:
                return $this->processAliasTransaction( $txkey, $tx );
            case TX_MASS_TRANSFER:
                return $this->processMassTransferTransaction( $txkey, $tx );
            case TX_DATA:
                return $this->processDataTransaction( $txkey, $tx );
            case TX_SMART_ACCOUNT:
                return $this->processSmartAccountTransaction( $txkey, $tx );
            case TX_SMART_ASSET:
                return $this->processSmartAssetTransaction( $txkey, $tx );
            case TX_SPONSORSHIP:
                return $this->processSponsorshipTransaction( $txkey, $tx );
            case TX_INVOKE:
                return $this->processInvokeTransaction( $txkey, $tx );
            case TX_UPDATE_ASSET_INFO:
                return $this->processUpdateAssetInfoTransaction( $txkey, $tx );
                
            default:
                w8io_error( 'unknown' );
        }
    }

    private function flush()
    {
        if( count( $this->recs ) )
        {
            $this->pts->merge( $this->recs );
            $this->blockchain->balances->update( $this->recs );
            $this->recs = [];

            foreach( $this->kvs as $kv )
                $kv->merge();
        }
    }

    public function rollback( $txfrom )
    {
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );
            foreach( $this->kvs as $kv )
                $kv->reset();

            //$beforeHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            //$beforeTxHeight = $this->txheight % W8IO_TXSHIFT;
            $this->setHighs();
            //$afterHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            //$afterTxHeight = $this->txheight % W8IO_TXSHIFT;
        }                    
        $this->db->commit();

        wk()->log( 'i', $beforeHeight . ':' . $beforeTxHeight . ' >> ' . $afterHeight . ':' . $afterTxHeight . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );
    }

    public function update( $txs )
    {
        // if global start not begin from FULL pts per block
        // append current PTS to track block fee
        foreach( $txs as $txkey => $tx )
            $this->processTransaction( $txkey, $tx );

        $this->flush();
    }
}

if( !isset( $lock ) )
    require_once '../w8io_updater.php';
