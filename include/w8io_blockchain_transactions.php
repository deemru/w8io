<?php

namespace w8io;

require_once 'Markers.php';

use deemru\WavesKit;
use deemru\Pairs;
use deemru\Triples;
use deemru\KV;

//require_once "w8io_base.php";
require_once "KV2.php";

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

define( 'GENESIS', 0 );
define( 'GENERATOR', -1 );
define( 'MATCHER', -2 );
define( 'UNDEFINED', -3 );
define( 'SPONSOR', -4 );
define( 'MASS', -5 );

define( 'TX_GENERATOR', 0 );
define( 'TX_GENESIS', 1 );
define( 'TX_PAYMENT', 2 );

define( 'WAVES_ASSET', 0 );

class BlockchainParser
{
    public Markers $markers;
    public Triples $db;
    public KV $kvAddresses;
    public Blockchain $blockchain;

    public function __construct( $writable = true )
    {
        $s = 'S:/w8io-refresh/parser.sqlite3';

        $this->markers = new Markers( $s );
        $this->db = $this->markers->db();

        $this->pts = new Triples( $this->db , 'pts', 1,
            // uid                 | txkey    | type     | a        | b        | asset    | amount   | feeasset | fee      | addon    | group
            // r0                  | r1       | r2       | r3       | r4       | r5       | r6       | r7       | r8       | r9       | r10
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         1,         1,         1,         1,         0,         1,         0,         0,         1 ] );

        $this->kvAddresses =    ( new KV( true ) )->setStorage( $this->db, 'addresses', true );
        $this->kvAliases =      ( new KV( false ) )->setStorage( $this->db, 'aliases', true, 'TEXT UNIQUE', 'INTEGER' );
        $this->kvAddons =       ( new KV( true ) )->setStorage( $this->db, 'addons', true );
        $this->kvAssets =       ( new KV( true ) )->setStorage( $this->db, 'assets', true );
        $this->kvAssetInfo =    ( new KV( false ) )->setStorage( $this->db, 'assetInfo', true );

        $this->kvs = [
            $this->kvAddresses,
            $this->kvAliases,
            $this->kvAddons,
            $this->kvAssets,
            $this->kvAssetInfo,
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

        $this->setUid();
        $this->setTxHeight();
        $this->recs = [];
    }

    private function setTxHeight()
    {
        $txheight = $this->pts->getHigh( 1 );
        $this->txheight = $txheight === false ? 0 : $txheight;
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

    public function get_hang_waves( $at )
    {
        if( $at <= W8IO_NG_ACTIVE )
            return 0;

        return $this->block_fees( $at, [], $this->get_wtxs_at( $at - 1 ) )[0];
    }

    public function set_fees( $block, $wtxs, $prev_wtxs )
    {
        $at = $block['height'];
        $fees = $this->block_fees( $at, $wtxs, $prev_wtxs );

        $wtx = [];
        $wtx['txid'] = $this->get_pair_txid( $at, true );
        $wtx['block'] = $at;
        $wtx['type'] = W8IO_TYPE_FEES;
        $wtx['timestamp'] = $this->timestamp( $block['timestamp'] );

        $wtx['fee'] = 0;
        $wtx['afee'] = W8IO_ASSET_EMPTY;
        $wtx['data'] = false;

        $wtx['a'] = 'GENERATOR';
        $wtx['b'] = $this->get_aid( $block['generator'], $at === 1 );

        foreach( $fees as $asset => $fee )
        {
            $wtx['asset'] = $asset;
            $wtx['amount'] = $fee;

            if( !$this->set_tx( $wtx ) )
                w8io_error();
        }

        return true;
    }

    private function set_transactions( $block )
    {
        $at = $block['height'];
        $txs = $block['transactions'];

        if( $at > W8IO_NG_ACTIVE )
            $prev_wtxs = isset( $this->wtx ) ? $this->wtx : $this->get_wtxs_at( $at - 1 );
        else
            $prev_wtxs = false;

        $this->wtxs = [];
        foreach( $txs as $tx )
            $this->set_transaction( $tx, $at );

        if( !$this->set_fees( $block, $this->wtxs, $prev_wtxs ) )
            w8io_error( "set_fees() failed" );

        $prev_wtxs = $this->wtxs;
        return true;
    }

    private function set_transaction( $tx, $at )
    {
        $type = $tx['type'];
        $wtx = [];
        $wtx['txid'] = $this->get_pair_txid( wk()->base58Decode( $tx['id'] ), true );
        $wtx['block'] = $at;
        $wtx['type'] = $type;
        $wtx['timestamp'] = $this->timestamp( $tx['timestamp'] );
        $wtx['amount'] = isset( $tx['amount'] ) ? $tx['amount'] : 0;
        $wtx['asset'] = 0;
        $wtx['fee'] = isset( $tx['fee'] ) ? $tx['fee'] : 0;
        $wtx['afee'] = 0;
        $wtx['data'] = false;

        switch( $type )
        {
            case 1: // genesis
                $wtx['a'] = 'GENESIS';
                $wtx['b'] = $tx['recipient'];
                break;
            case 101: // genesis role
                $wtx['a'] = 'GENESIS';
                $wtx['b'] = $tx['target'];
                $wtx['data'] = [ 'd' => $this->get_dataid( $tx['role'], true ) ];
                break;
            case 102: // role
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['target'];
                //$wtx['data'] = [ 'opType' => $this->get_dataid( $tx['opType'], true ) ];
                $wtx['data'] = [ 'd' => $this->get_dataid( $tx['role'], true ) ];
                //$wtx['data'] = [ 'dueTimestamp' => $this->get_dataid( $tx['dueTimestamp'], true ) ];
                break;
            case 110: // genesis unknown
                $wtx['a'] = 'GENESIS';
                $wtx['b'] = $tx['target'];
                break;
            case 105: // data unknown
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                break;
            case 106: // invoke 1 unknown
            case 107: // invoke 2 unknown
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                break;
            case 2: // payment
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                break;
            case 3: // issue
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['amount'] = $tx['quantity'];
                {
                    $asset = $this->get_assetid( $tx['assetId'], true );

                    $tx['name'] = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
                    if( $this->pairs_asset_info->setKeyValue( $asset, $tx, 'j' ) === false )
                        w8io_error();

                    $wtx['asset'] = $asset;
                }
                break;

            case W8IO_TYPE_INVOKE_TRANSFER:
            case 4: // transfer
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                {
                    $attachment = $tx['attachment'];
                    if( is_string( $attachment ) && strlen( $attachment ) > 0 )
                        $wtx['data'] = [ 'd' => $this->get_dataid( $attachment, true ) ];
                }
                {
                    if( null !== ( $asset = $tx['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );

                    if( null !== ( $asset = $tx['feeAssetId'] ) )
                        $wtx['afee'] = $this->get_assetid( $asset );
                }
                break;
            case 5: // reissue
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['amount'] = $tx['quantity'];
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;
            case 6: // burn
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;
            case 7: // exchange
                {
                    $buyer = $tx['order1'];
                    $seller = $tx['order2'];
                    $ba = $this->get_aid( $buyer['sender'] );
                    $sa = $this->get_aid( $seller['sender'] );

                    $basset = $buyer['assetPair']['amountAsset'];
                    $basset = $basset !== null ? $this->get_assetid( $basset ) : 0;

                    $sasset = $buyer['assetPair']['priceAsset'];
                    $sasset = $sasset !== null ? $this->get_assetid( $sasset ) : 0;
                }
                {
                    $bfee = $tx['buyMatcherFee'];
                    $sfee = $tx['sellMatcherFee'];
                    $fee = $tx['fee'];
                    $amount = $tx['amount'];
                }
                // MATCHER;
                {
                    $wtx['a'] = 'MATCHER';
                    $wtx['b'] = $tx['sender'];
                    $wtx['amount'] = $bfee + $sfee - $fee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // SELLER -> BUYER
                {
                    $wtx['a'] = $sa;
                    $wtx['b'] = $ba;
                    $wtx['amount'] = $amount;
                    $wtx['asset'] = $basset;
                    $wtx['fee'] = $sfee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // BUYER -> SELLER
                {
                    $wtx['a'] = $ba;
                    $wtx['b'] = $sa;
                    $wtx['amount'] = gmp_intval( gmp_div( gmp_mul( $tx['price'], $amount ), 100000000 ) );
                    $wtx['asset'] = $sasset;
                    $wtx['fee'] = $bfee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                return;

            case 8: // start lease
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                break;
            case 9: // cancel lease
                $wtx['a'] = $tx['sender'];
                {
                    $wtxs = $this->get_txid( $this->get_pair_txid( wk()->base58Decode( $tx['leaseId'] ) ) );
                    if( $wtxs === false )
                        w8io_error();

                    $wtx_lease = $wtxs[0];
                    $wtx['b'] = $wtx_lease['b'];

                    if( count( $wtxs ) === 1 )
                    {
                        $wtx['txid'] = $wtx_lease['txid'];
                        $wtx['amount'] = $wtx_lease['amount'];
                    }
                    else
                    {
                        $wtx['amount'] = 0; // already cancelled
                    }
                }
                break;
            case 10: // alias
                $wtx['b'] = 'NULL';
                {
                    $aid = $this->get_aid( $tx['sender'], $wtx['fee'] === 0 ? true : false );

                    $wtx['a'] = $aid;

                    $alias = $tx['alias'];

                    if( false === $this->pairs_aliases->setKeyValue( $alias, $aid ) )
                        w8io_error();

                    $wtx['data'] = [ 'd' => $this->get_dataid( $alias, true ) ];
                }
                break;
            case 11: // mass transfer
                // SENDER
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'MASS';
                $wtx['amount'] = $tx['totalAmount'];
                {
                    if( null !== ( $asset = $tx['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // RECIPIENTS
                $wtx['fee'] = 0;
                $mtxs = $tx['transfers'];
                foreach( $mtxs as $mtx )
                {
                    $wtx['b'] = $mtx['recipient'];
                    $wtx['amount'] = $mtx['amount'];

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                return;
            
            case W8IO_TYPE_INVOKE_DATA:
            case 12: // data
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['data'] = [ 'd' => $this->get_dataid( json_encode( $tx['data'] ), true ) ];
                break;

            case 13: // smart account
            case 15: // smart asset
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['data'] = [ 's' => $this->get_dataid( json_encode( $tx['script'] ), true ) ];
                if( $type === 15 )
                    $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;

            case 14: // sponsorship
                $wtx['a'] = $this->get_aid( $tx['sender'] );
                $wtx['b'] = 'NULL';
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                $wtx['amount'] = $tx['minSponsoredAssetFee'];
                $this->set_sponsor( $wtx['asset'], $wtx['a'], $wtx['amount'] );
                break;

            case 16: // invoke
            {
                if( false === ( $stateChanges = wk()->getStateChanges( $tx['id'] ) ) )
                    w8io_error( "getStateChanges( {$tx['id']} ) failed" );
                if( $tx['id'] !== $stateChanges['id'] )
                    w8io_error( "tx vs. ftx diff found ({$tx['id']})" );

                $wtx['a'] = $tx['sender'];

                if( strlen( $tx['dApp'] ) === 35 )
                {
                    $dAppAddress = $tx['dApp'];
                    $wtx['b'] = $dAppAddress;
                }
                else
                {
                    $alias = $tx['dApp'];
                    if( substr( $alias, 0, 6 ) !== 'alias:' )
                        $alias = substr( wk()->base58Decode( $alias ), 4 );
                    else
                        $alias = substr( $alias, 8 );

                    $wtx['b'] = 'alias:#:' . $alias;

                    $alias = $this->pairs_aliases->getValue( $alias, 'i' );
                    if( $alias === false )
                        w8io_error();

                    $dAppAddress = $this->pairs_addresses->getValue( $alias );
                    if( $dAppAddress === false )
                        w8io_error();
                }

                if( isset( $tx['payment'][0] ) )
                {
                    $payment = $tx['payment'][0];
                    if( null !== ( $asset = $payment['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );
                    $wtx['amount'] = $payment['amount'];
                }

                if( null !== ( $asset = $tx['feeAssetId'] ) )
                    $wtx['afee'] = $this->get_assetid( $asset );

                if( isset( $tx['call'] ) )
                {
                    $call = [ $tx['call']['function'] => $tx['call']['args'] ];
                    $wtx['data'] = [ 'c' => $this->get_dataid( json_encode( $call ), true ) ];
                }

                if( !$this->set_tx( $wtx ) )
                    w8io_error();

                $stateChanges = $stateChanges['stateChanges'];
                $data = $stateChanges['data'];
                $transfers = $stateChanges['transfers'];

                if( count( $data ) || count( $transfers ) )
                {
                    $tx['sender'] = $dAppAddress;
                    $tx['fee'] = 0;
                    $tx['feeAssetId'] = null;

                    if( count( $data ) )
                    {
                        $tx['type'] = W8IO_TYPE_INVOKE_DATA;
                        $tx['data'] = $data;
                        $this->set_transaction( $tx, $at );
                    }

                    if( count( $transfers ) )
                    {
                        $tx['type'] = W8IO_TYPE_INVOKE_TRANSFER;
                        $tx['attachment'] = null;
                        foreach( $transfers as $transfer )
                        {
                            $tx['recipient'] = $transfer['address'];
                            $tx['assetId'] = $transfer['asset'];
                            $tx['amount'] = $transfer['amount'];
                            $this->set_transaction( $tx, $at );
                        }
                    }
                }

                return;
            }

            default:
                w8io_error( json_encode( $wtx ) );
        }

        if( $this->set_tx( $wtx ) === false )
            w8io_error( "set_tx() failed" );
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
            w8io_error( 'getExistingAddressId' );
        
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
            w8io_error( 'getValueByKey' );
        
        return $id;
    }

    private function processGeneratorTransaction( $txkey, $header )
    {
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_GENERATOR,
            A =>        $this->getSenderId( 'GENERATOR' ),
            B =>        $this->getRecipientId( $header['generator'] ),
            ASSET =>    WAVES_ASSET,
            AMOUNT =>   $header['fee'],
            FEEASSET => WAVES_ASSET,
            FEE =>      0,
            ADDON =>    0,
            GROUP =>    0,
        ];
    }

    private function processGenesisTransaction( $txkey, $tx )
    {
        $this->recs[] = [
            UID =>      $this->getNewUid(),
            TXKEY =>    $txkey,
            TYPE =>     TX_GENESIS,
            A =>        $this->getSenderId( 'GENESIS' ),
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

    private function processTransaction( $txkey, $tx )
    {
        switch( $tx['type'] )
        {
            case TX_GENESIS:
                return $this->processGenesisTransaction( $txkey, $tx );
            case TX_PAYMENT:
                return $this->processPaymentTransaction( $txkey, $tx );
            default:
                w8io_error( 'unknown' );
        }
        

        $wtx[TXKEY] = $this->get_pair_txid( wk()->base58Decode( $tx['id'] ), true );
        $wtx[TYPE] = $type;
        $wtx[AMOUNT] = isset( $tx['amount'] ) ? $tx['amount'] : 0;
        $wtx['asset'] = 0;
        $wtx[FEE] = isset( $tx['fee'] ) ? $tx['fee'] : 0;
        $wtx['afee'] = 0;
        $wtx['data'] = false;

        switch( $type )
        {
            
            case 2: // payment
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                break;
            case 3: // issue
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['amount'] = $tx['quantity'];
                {
                    $asset = $this->get_assetid( $tx['assetId'], true );

                    $tx['name'] = htmlentities( trim( preg_replace( '/\s+/', ' ', $tx['name'] ) ) );
                    if( $this->pairs_asset_info->setKeyValue( $asset, $tx, 'j' ) === false )
                        w8io_error();

                    $wtx['asset'] = $asset;
                }
                break;

            case W8IO_TYPE_INVOKE_TRANSFER:
            case 4: // transfer
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                {
                    $attachment = $tx['attachment'];
                    if( is_string( $attachment ) && strlen( $attachment ) > 0 )
                        $wtx['data'] = [ 'd' => $this->get_dataid( $attachment, true ) ];
                }
                {
                    if( null !== ( $asset = $tx['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );

                    if( null !== ( $asset = $tx['feeAssetId'] ) )
                        $wtx['afee'] = $this->get_assetid( $asset );
                }
                break;
            case 5: // reissue
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['amount'] = $tx['quantity'];
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;
            case 6: // burn
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;
            case 7: // exchange
                {
                    $buyer = $tx['order1'];
                    $seller = $tx['order2'];
                    $ba = $this->get_aid( $buyer['sender'] );
                    $sa = $this->get_aid( $seller['sender'] );

                    $basset = $buyer['assetPair']['amountAsset'];
                    $basset = $basset !== null ? $this->get_assetid( $basset ) : 0;

                    $sasset = $buyer['assetPair']['priceAsset'];
                    $sasset = $sasset !== null ? $this->get_assetid( $sasset ) : 0;
                }
                {
                    $bfee = $tx['buyMatcherFee'];
                    $sfee = $tx['sellMatcherFee'];
                    $fee = $tx['fee'];
                    $amount = $tx['amount'];
                }
                // MATCHER;
                {
                    $wtx['a'] = 'MATCHER';
                    $wtx['b'] = $tx['sender'];
                    $wtx['amount'] = $bfee + $sfee - $fee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // SELLER -> BUYER
                {
                    $wtx['a'] = $sa;
                    $wtx['b'] = $ba;
                    $wtx['amount'] = $amount;
                    $wtx['asset'] = $basset;
                    $wtx['fee'] = $sfee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // BUYER -> SELLER
                {
                    $wtx['a'] = $ba;
                    $wtx['b'] = $sa;
                    $wtx['amount'] = gmp_intval( gmp_div( gmp_mul( $tx['price'], $amount ), 100000000 ) );
                    $wtx['asset'] = $sasset;
                    $wtx['fee'] = $bfee;

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                return;

            case 8: // start lease
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = $tx['recipient'];
                break;
            case 9: // cancel lease
                $wtx['a'] = $tx['sender'];
                {
                    $wtxs = $this->get_txid( $this->get_pair_txid( wk()->base58Decode( $tx['leaseId'] ) ) );
                    if( $wtxs === false )
                        w8io_error();

                    $wtx_lease = $wtxs[0];
                    $wtx['b'] = $wtx_lease['b'];

                    if( count( $wtxs ) === 1 )
                    {
                        $wtx['txid'] = $wtx_lease['txid'];
                        $wtx['amount'] = $wtx_lease['amount'];
                    }
                    else
                    {
                        $wtx['amount'] = 0; // already cancelled
                    }
                }
                break;
            case 10: // alias
                $wtx['b'] = 'NULL';
                {
                    $aid = $this->get_aid( $tx['sender'], $wtx['fee'] === 0 ? true : false );

                    $wtx['a'] = $aid;

                    $alias = $tx['alias'];

                    if( false === $this->pairs_aliases->setKeyValue( $alias, $aid ) )
                        w8io_error();

                    $wtx['data'] = [ 'd' => $this->get_dataid( $alias, true ) ];
                }
                break;
            case 11: // mass transfer
                // SENDER
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'MASS';
                $wtx['amount'] = $tx['totalAmount'];
                {
                    if( null !== ( $asset = $tx['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                // RECIPIENTS
                $wtx['fee'] = 0;
                $mtxs = $tx['transfers'];
                foreach( $mtxs as $mtx )
                {
                    $wtx['b'] = $mtx['recipient'];
                    $wtx['amount'] = $mtx['amount'];

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();
                }
                return;
            
            case W8IO_TYPE_INVOKE_DATA:
            case 12: // data
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['data'] = [ 'd' => $this->get_dataid( json_encode( $tx['data'] ), true ) ];
                break;

            case 13: // smart account
            case 15: // smart asset
                $wtx['a'] = $tx['sender'];
                $wtx['b'] = 'NULL';
                $wtx['data'] = [ 's' => $this->get_dataid( json_encode( $tx['script'] ), true ) ];
                if( $type === 15 )
                    $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                break;

            case 14: // sponsorship
                $wtx['a'] = $this->get_aid( $tx['sender'] );
                $wtx['b'] = 'NULL';
                $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                $wtx['amount'] = $tx['minSponsoredAssetFee'];
                $this->set_sponsor( $wtx['asset'], $wtx['a'], $wtx['amount'] );
                break;

            case 16: // invoke
            {
                if( false === ( $stateChanges = wk()->getStateChanges( $tx['id'] ) ) )
                    w8io_error( "getStateChanges( {$tx['id']} ) failed" );
                if( $tx['id'] !== $stateChanges['id'] )
                    w8io_error( "tx vs. ftx diff found ({$tx['id']})" );

                $wtx['a'] = $tx['sender'];

                if( strlen( $tx['dApp'] ) === 35 )
                {
                    $dAppAddress = $tx['dApp'];
                    $wtx['b'] = $dAppAddress;
                }
                else
                {
                    $alias = $tx['dApp'];
                    if( substr( $alias, 0, 6 ) !== 'alias:' )
                        $alias = substr( wk()->base58Decode( $alias ), 4 );
                    else
                        $alias = substr( $alias, 8 );

                    $wtx['b'] = 'alias:#:' . $alias;

                    $alias = $this->pairs_aliases->getValue( $alias, 'i' );
                    if( $alias === false )
                        w8io_error();

                    $dAppAddress = $this->pairs_addresses->getValue( $alias );
                    if( $dAppAddress === false )
                        w8io_error();
                }

                if( isset( $tx['payment'][0] ) )
                {
                    $payment = $tx['payment'][0];
                    if( null !== ( $asset = $payment['assetId'] ) )
                        $wtx['asset'] = $this->get_assetid( $asset );
                    $wtx['amount'] = $payment['amount'];
                }

                if( null !== ( $asset = $tx['feeAssetId'] ) )
                    $wtx['afee'] = $this->get_assetid( $asset );

                if( isset( $tx['call'] ) )
                {
                    $call = [ $tx['call']['function'] => $tx['call']['args'] ];
                    $wtx['data'] = [ 'c' => $this->get_dataid( json_encode( $call ), true ) ];
                }

                if( !$this->set_tx( $wtx ) )
                    w8io_error();

                $stateChanges = $stateChanges['stateChanges'];
                $data = $stateChanges['data'];
                $transfers = $stateChanges['transfers'];

                if( count( $data ) || count( $transfers ) )
                {
                    $tx['sender'] = $dAppAddress;
                    $tx['fee'] = 0;
                    $tx['feeAssetId'] = null;

                    if( count( $data ) )
                    {
                        $tx['type'] = W8IO_TYPE_INVOKE_DATA;
                        $tx['data'] = $data;
                        $this->set_transaction( $tx, $at );
                    }

                    if( count( $transfers ) )
                    {
                        $tx['type'] = W8IO_TYPE_INVOKE_TRANSFER;
                        $tx['attachment'] = null;
                        foreach( $transfers as $transfer )
                        {
                            $tx['recipient'] = $transfer['address'];
                            $tx['assetId'] = $transfer['asset'];
                            $tx['amount'] = $transfer['amount'];
                            $this->set_transaction( $tx, $at );
                        }
                    }
                }

                return;
            }

            default:
                w8io_error( json_encode( $wtx ) );
        }

        if( $this->set_tx( $wtx ) === false )
            w8io_error( "set_tx() failed" );
    }

    private function commit()
    {
        $tt = microtime( true );
        $this->db->begin();
        {
            if( count( $this->recs ) )
            {
                $this->pts->merge( $this->recs );
                $this->recs = [];
            }            

            foreach( $this->kvs as $kv )
                $kv->merge();

            $beforeHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            $beforeTxHeight = $this->txheight % W8IO_TXSHIFT;
            $this->setTxHeight();
            $afterHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            $afterTxHeight = $this->txheight % W8IO_TXSHIFT;

            $this->markers->setMarkers( null, $this->txheight );
        }
        $this->db->commit();

        wk()->log( 'i', $beforeHeight . ':' . $beforeTxHeight . ' >> ' . $afterHeight . ':' . $afterTxHeight . ' (commit) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );        

        // $this->blockchain->markers->setMarkers( $this->txheight + 1, null, true );
    }

    public function rollback( $txfrom )
    {
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->pts->query( 'DELETE FROM pts WHERE r1 >= '. $txfrom );
            $this->markers->setMarkers( $txfrom, $txfrom );
            foreach( $this->kvs as $kv )
                $kv->reset();

            $beforeHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            $beforeTxHeight = $this->txheight % W8IO_TXSHIFT;
            $this->setTxHeight();
            $afterHeight  = intdiv( $this->txheight, W8IO_TXSHIFT );
            $afterTxHeight = $this->txheight % W8IO_TXSHIFT;

            $this->markers->setMarkers( $this->txheight, $this->txheight );
        }                    
        $this->db->commit();

        wk()->log( 'i', $beforeHeight . ':' . $beforeTxHeight . ' >> ' . $afterHeight . ':' . $afterTxHeight . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );        
    }

    public function update()
    {
        $lo = $this->blockchain->markers->getLoMarker();
        $hi = $this->blockchain->markers->getHiMarker();

        if( $this->txheight > $lo )
            $this->rollback( $lo );

        $from = $lo;
        $to = $from - ( $from % W8IO_TXSHIFT ) + W8IO_TXSHIFT - 1;
        $n = 0;
        $t = 0;
        for( $i = 0; $i < W8IO_MAX_UPDATE_BATCH; ++$i )
        {
            $txs = $this->blockchain->getTransactionsFromTo( $from, $to );
                
            foreach( $txs as $txkey => $tx )
                $this->processTransaction( $txkey, $tx );

            $this->processGeneratorTransaction( $to, $this->blockchain->getHeaderAt( intdiv( $from, W8IO_TXSHIFT ) ) );

            $from = $to + 1;
            $to += W8IO_TXSHIFT;
        }

        $this->commit();            

        return $this->txheight !== $hi; 
    }
}

if( !isset( $lock ) )
    require_once '../w8io_updater.php';
