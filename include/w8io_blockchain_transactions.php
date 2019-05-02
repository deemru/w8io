<?php

use deemru\WavesKit;
use deemru\Pairs;

require_once "w8io_base.php";

class w8io_blockchain_transactions
{
    private $transactions;
    private $checkpoint;

    private $query_get_txs_all;
    private $query_get_txs;
    private $query_get_txs_asset;
    private $query_get_txid;
    private $query_set_tx;
    private $query_clear;
    private $query_from_to;
    private $query_wtxs_at;

    private $pairs_txids;
    private $pairs_addresses;
    private $pairs_assets;
    private $pairs_asset_info;
    private $pairs_aliases;

    private $sponsors;
    private $wtxs;

    public function __construct( $writable = true )
    {
        $this->transactions = new PDO( 'sqlite:' . W8IO_DB_BLOCKCHAIN_TRANSACTIONS );
        if( !$this->transactions->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING ) )
            w8io_error( 'PDO->setAttribute()' );

        $this->transactions->exec( W8IO_DB_PRAGMAS );

        if( $writable )
        {
            $this->checkpoint = new Pairs( $this->transactions, 'checkpoint', $writable, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->pairs_txids = new Pairs( $this->transactions, 'txids', true );
            $this->pairs_addresses = new Pairs( $this->transactions, 'addresses', true );
            $this->pairs_assets = new Pairs( $this->transactions, 'assets', true );
            $this->pairs_asset_info = new Pairs( $this->transactions, 'asset_info', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->pairs_aliases = new Pairs( $this->transactions, 'aliases', true, 'TEXT PRIMARY KEY|INTEGER|0|1' );
            $this->pairs_addons = new Pairs( $this->transactions, 'addons', true );

            $this->transactions->exec( W8IO_DB_WRITE_PRAGMAS );
            $this->transactions->exec( "CREATE TABLE IF NOT EXISTS transactions (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                txid INTEGER,
                block INTEGER,
                type INTEGER,
                timestamp INTEGER,
                a INTEGER,
                b INTEGER,
                amount INTEGER,
                asset INTEGER,
                fee INTEGER,
                afee INTEGER,
                data TEXT )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_txid  ON transactions( txid )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_block ON transactions( block )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_a     ON transactions( a )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_b     ON transactions( b )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_as    ON transactions( asset )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_asa   ON transactions( a, asset )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_asb   ON transactions( b, asset )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_t     ON transactions( type )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_ta    ON transactions( a, type )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_tb    ON transactions( b, type )" );

            $this->pairs_addresses->setKeyValue(  0, 'GENESIS' );
            $this->pairs_addresses->setKeyValue( -1, 'GENERATOR' );
            $this->pairs_addresses->setKeyValue( -2, 'MATCHER' );
            $this->pairs_addresses->setKeyValue( -3, 'NULL' );
            $this->pairs_addresses->setKeyValue( -4, 'SPONSOR' );
        }
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
        global $wk;

        $at = $block['height'];
        $txs = $block['transactions'];

        if( $at > W8IO_NG_ACTIVE )
            $prev_wtxs = isset( $this->wtx ) ? $this->wtx : $this->get_wtxs_at( $at - 1 );
        else
            $prev_wtxs = false;

        $this->wtxs = [];

        foreach( $txs as $tx )
        {
            $type = $tx['type'];
            $wtx = [];
            $wtx['txid'] = $this->get_pair_txid( $wk->base58Decode( $tx['id'] ), true );
            $wtx['block'] = $at;
            $wtx['type'] = $type;
            $wtx['timestamp'] = $this->timestamp( $tx['timestamp'] );
            $wtx['amount'] = isset( $tx['amount'] ) ? $tx['amount'] : 0;
            $wtx['asset'] = 0;
            $wtx['fee'] = isset( $tx['fee'] ) ? $tx['fee'] : 0;
            $wtx['afee'] = 0;
            $wtx['data'] = false;
            $saved = false;

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
                    $saved = true;
                    break;
                case 8: // start lease
                    $wtx['a'] = $tx['sender'];
                    $wtx['b'] = $tx['recipient'];
                    break;
                case 9: // cancel lease
                    $wtx['a'] = $tx['sender'];
                    {
                        $wtxs = $this->get_txid( $this->get_pair_txid( $wk->base58Decode( $tx['leaseId'] ) ) );
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
                    $wtx['b'] = 'NULL';
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
                    $saved = true;
                    break;
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
                    if( $type == 15 )
                        $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                    break;

                case 14: // sponsorship
                    $wtx['a'] = $this->get_aid( $tx['sender'] );
                    $wtx['b'] = 'NULL';
                    $wtx['asset'] = $this->get_assetid( $tx['assetId'] );
                    $wtx['amount'] = $tx['minSponsoredAssetFee'];
                    $this->set_sponsor( $wtx['asset'], $wtx['a'], $wtx['amount'] );
                    break;

                default:
                    w8io_error( json_encode( $wtx ) );
            }

            if( $saved === false && $this->set_tx( $wtx ) === false )
                w8io_error( "set_tx() failed" );
        }

        if( !$this->set_fees( $block, $this->wtxs, $prev_wtxs ) )
            w8io_error( "set_fees() failed" );

        $prev_wtxs = $this->wtxs;
        return true;
    }

    public function update( $upcontext, $balances )
    {
        $blockchain = $upcontext['blockchain'];
        $from = $upcontext['from'];
        $to = $upcontext['to'];
        $local_height = $this->get_height();

        if( $local_height !== $from )
        {
            $from = min( $local_height, $from );

            if( $local_height > $from )
            // ROLLBACK
            {
                $balances->rollback( $this, $from );
                w8io_warning( "transactions (rollback to $from)" );

                for( $i = $local_height;; )
                {
                    $i -= W8IO_MAX_UPDATE_BATCH;
                    $i = max( $from, $i );
                    w8io_info( "transactions (rollback to $i)" );

                    if( !$this->transactions->beginTransaction() )
                        w8io_error( 'unexpected begin() error' );
                    if( !$this->clear_transactions( $i ) )
                        w8io_error( 'unexpected clear_transactions() error' );
                    if( false === $this->checkpoint->setKeyValue( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, $i ) )
                        w8io_error( 'set checkpoint_transactions failed' );
                    if( !$this->transactions->commit() )
                        w8io_error( 'unexpected commit() error' );

                    if( $i === $from )
                        break;
                }
                unset( $this->sponsors );
                w8io_trace( 's', "transactions (rollback to $from) (done)" );
            }
        }

        $to = min( $to, $from + W8IO_MAX_UPDATE_BATCH );

        if( !isset( $this->sponsors ) )
            $this->fill_sponsors();

        if( !$this->transactions->beginTransaction() )
            w8io_error( 'unexpected begin() error' );

        for( $i = $from + 1; $i <= $to; $i++ )
        {
            w8io_trace( 'i', "$i (transactions)" );

            $block = $blockchain->get_block( $i );
            if( $block === false )
                w8io_error( 'unexpected blockchain->get_block() error' );

            if( !$this->set_transactions( $block ) )
                w8io_error( 'unexpected set_transactions() corruption' );
        }

        if( false === $this->checkpoint->setKeyValue( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, $to ) )
            w8io_error( 'set checkpoint_transactions failed' );

        if( !$this->transactions->commit() )
            w8io_error( 'unexpected commit() error' );

        return [ 'transactions' => $this, 'from' => $from, 'to' => $to ];
    }
}
