<?php

require_once 'w8io_blockchain.php';

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

    private $crypto;
    private $pairs_txids;
    private $pairs_addresses;
    private $pairs_pubkey_addresses;
    private $pairs_assets;
    private $pairs_asset_info;
    private $pairs_aliases;
    private $pairs_lease_info;

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
            $this->checkpoint = new w8io_pairs( $this->transactions, 'checkpoint', $writable, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->pairs_txids = new w8io_pairs( $this->transactions, 'txids', true );
            $this->pairs_addresses = new w8io_pairs( $this->transactions, 'addresses', true );
            $this->pairs_pubkey_addresses = new w8io_pairs( $this->transactions, 'pubkey_addresses', true, 'TEXT PRIMARY KEY|INTEGER|0|0' );
            $this->pairs_assets = new w8io_pairs( $this->transactions, 'assets', true );
            $this->pairs_asset_info = new w8io_pairs( $this->transactions, 'asset_info', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->pairs_aliases = new w8io_pairs( $this->transactions, 'aliases', true, 'TEXT PRIMARY KEY|INTEGER|0|1' );
            $this->pairs_addons = new w8io_pairs( $this->transactions, 'addons', true );
            $this->pairs_lease_info = new w8io_pairs( $this->transactions, 'lease_info', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );

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
        }
    }

    private function get_txid( $txid, $one = false )
    {
        if( !isset( $this->query_get_txid ) )
        {
            $this->query_get_txid = $this->transactions->prepare( "SELECT * FROM transactions WHERE txid = :txid" );
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

            return $data[0];
        }

        return $data;
    }

    private function get_wtxs_at( $at )
    {
        $query_wtxs_at = $this->transactions->prepare( "SELECT * FROM transactions WHERE block = $at" );
        if( !is_object( $query_wtxs_at ) )
            return false;

        if( $query_wtxs_at->execute() === false )
            return false;

        $query_wtxs_at->fetchAll( PDO::FETCH_ASSOC );

        return array_map( 'self::filter_wtx', $query_wtxs_at );
    }

    public function get_height()
    {
        $height = $this->checkpoint->get_value( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, 'i' );
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
        if( false === ( $id = $this->pairs_txids->get_id( $id, $new ) ) )
            w8io_error();
        return $id;
    }

    private function get_assetid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_assets->get_id( $id, $new ) ) )
            w8io_error( $id );
        return $id;
    }

    private function get_dataid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_addons->get_id( $id, $new ) ) )
            w8io_error();
        return $id;
    }

    private function get_aid( $id, $new = false )
    {
        if( false === ( $id = $this->pairs_addresses->get_id( $id, $new ) ) )
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

    public function get_txs_where( $aid, $where, $limit = 100 )
    {
        if( $aid !== false )
        {
            $where = $where ? "AND $where" : '';
            $where =
                "SELECT * FROM ( SELECT * FROM transactions WHERE a = $aid $where ORDER BY uid DESC LIMIT $limit ) UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE b = $aid $where ORDER BY uid DESC LIMIT $limit ) ORDER BY uid DESC LIMIT $limit";
        }
        else
        {
            $where = $where ? "WHERE $where" : '';
            $where =
                "SELECT * FROM transactions $where ORDER BY uid DESC LIMIT $limit";
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

        return $this->query_from_to;
    }

    public function mark_scam( $scam, $mark )
    {
        $id = $this->pairs_assets->get_id( $scam );
        if( $id === false )
            return;

        $info = $this->pairs_asset_info->get_value( $id, 'j' );
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

        $this->pairs_asset_info->set_pair( $id, $info, 'j' );
    }

    public function mark_tickers( $ticker, $mark )
    {
        $id = $this->pairs_assets->get_id( $ticker );
        if( $id === false )
            return;

        $info = $this->pairs_asset_info->get_value( $id, 'j' );
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

        $this->pairs_asset_info->set_pair( $id, $info, 'j' );
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
                $a = $this->pairs_addresses->get_id( $wtx['a'], true );
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

                $b = $this->pairs_aliases->get_value( $alias, 'i' );
                if( $b === false )
                    return false;

                if( $wtx['data'] !== false )
                    $wtx['data']['b'] = $this->get_dataid( $alias );
                else
                    $wtx['data'] = [ 'b' => $this->get_dataid( $alias ) ];
            }
            else if( $b[0] === '3' )
            {
                $b = $this->pairs_addresses->get_id( $wtx['b'], true );
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

    private function filter_wtx( $wtx )
    {
        if( isset( $wtx['uid'] ) )
            $wtx['uid'] = (int)$wtx['uid'];
        $wtx['txid'] = (int)$wtx['txid'];
        $wtx['block'] = (int)$wtx['block'];
        $wtx['type'] = (int)$wtx['type'];
        $wtx['timestamp'] = (int)$wtx['timestamp'];
        $wtx['a'] = (int)$wtx['a'];
        $wtx['b'] = (int)$wtx['b'];
        $wtx['amount'] = (int)$wtx['amount'];
        $wtx['asset'] = (int)$wtx['asset'];
        $wtx['fee'] = (int)$wtx['fee'];
        $wtx['afee'] = (int)$wtx['afee'];
        $wtx['data'] = empty( $wtx['data'] ) ? false : $wtx['data'];
        return $wtx;
    }

    private function block_fees( $at, $wtxs, $prev_wtxs )
    {
        $fees = [];
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
                    $ngfee = intdiv( $fee, 5 ) * 2;

                    if( $asset > 0 && $at >= W8IO_SPONSOR_ACTIVE && $wtx['data'] !== false )
                    {
                        $data = json_decode( $wtx['data'], true );
                        if( isset( $data['f'] ) )
                        {
                            $asset = 0;
                            $ngfee = gmp_intval( gmp_div( gmp_mul( $ngfee, 100000 ), $data['f'] ) );
                            if( $prev )
                                $fee = gmp_intval( gmp_div( gmp_mul( $fee, 100000 ), $data['f'] ) );
                        }
                    }

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

        $fees = $this->block_fees( $at, [], $this->get_wtxs_at( $at - 1 ) );
        if( isset( $fees[0] ) )
            return $fees[0];

        return 0;
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

        if( count( $fees ) === 0 )
        {
            $wtx['amount'] = 0;
            $wtx['asset'] = 0;

            if( !$this->set_tx( $wtx ) )
                w8io_error();
        }
        else
        foreach( $fees as $asset => $fee )
        {
            $wtx['asset'] = $asset;
            $wtx['amount'] = $fee;

            if( !$this->set_tx( $wtx ) )
                w8io_error();
        }

        return true;
    }

    private function get_crypto()
    {
        if( !isset( $this->crypto ) )
        {
            require_once 'w8io_crypto.php';
            $this->crypto = new w8io_crypto();
        }

        return $this->crypto;
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
        {
            $type = $tx['type'];
            $wtx = [];
            $wtx['txid'] = $this->get_pair_txid( $tx['id'], true );
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
                        if( $this->pairs_asset_info->set_pair( $asset, $tx, 'j' ) === false )
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

                        if( null !== ( $asset = $tx['feeAsset'] ) )
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

                        $pub = $buyer['senderPublicKey'];
                        $ba = $this->pairs_pubkey_addresses->get_value( $pub, 'i' );
                        if( $ba === false )
                        {
                            $ba = $this->get_crypto()->get_address_from_pubkey( $pub );
                            if( $ba === false )
                                w8io_error();

                            $ba = $this->get_aid( $ba );
                            if( $this->pairs_pubkey_addresses->set_pair( $pub, $ba ) === false )
                                w8io_error();
                        }

                        $pub = $seller['senderPublicKey'];
                        $sa = $this->pairs_pubkey_addresses->get_value( $pub, 'i' );
                        if( $sa === false )
                        {
                            $sa = $this->get_crypto()->get_address_from_pubkey( $pub );
                            if( $sa === false )
                                w8io_error();

                            $sa = $this->get_aid( $sa );
                            if( $this->pairs_pubkey_addresses->set_pair( $pub, $sa ) === false )
                                w8io_error();
                        }

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

                    if( !$this->set_tx( $wtx ) )
                        w8io_error();

                    $saved = true;
                    $wtx = end( $this->wtxs );

                    if( false === $this->pairs_lease_info->set_pair( $wtx['txid'], [ '$' => $wtx['amount'], 'b' => $wtx['b'] ], 'j' ) )
                        w8io_error();

                    break;
                case 9: // cancel lease
                    $wtx['a'] = $tx['sender'];
                    {
                        $txid = $this->get_pair_txid( $tx['leaseId'] );
                        $lease_info = $this->pairs_lease_info->get_value( $txid, 'j' );
                        if( $lease_info === false )
                            w8io_error();

                        $wtx['b'] = intval( $lease_info['b'] );

                        if( !isset( $lease_info['x'] ) || false === $this->get_txid( $lease_info['x'], true ) )
                        {
                            $wtx['amount'] = $lease_info['$'];

                            $lease_info['x'] = $wtx['txid'];
                            if( false === $this->pairs_lease_info->set_pair( $txid, $lease_info, 'j' ) )
                                w8io_error();
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
                        $aid = $this->get_aid( $tx['sender'] );

                        $wtx['a'] = $aid;

                        $alias = $tx['alias'];

                        if( false === $this->pairs_aliases->set_pair( $alias, $aid ) )
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

                case 13: // script
                    $wtx['a'] = $tx['sender'];
                    $wtx['b'] = 'NULL';
                    $wtx['data'] = [ 's' => $this->get_dataid( json_encode( $tx['script'] ), true ) ];
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

    public function update( $upcontext )
    {
        // TODO
        {
            $this->pairs_addresses->set_pair(  0, 'GENESIS' );
            $this->pairs_addresses->set_pair( -1, 'GENERATOR' );
            $this->pairs_addresses->set_pair( -2, 'MATCHER' );
            $this->pairs_addresses->set_pair( -3, 'NULL' );
            $this->pairs_addresses->set_pair( -4, 'SPONSOR' );
        }

        $blockchain = $upcontext['blockchain'];
        $from = $upcontext['from'];
        $to = $upcontext['to'];
        $local_height = $this->get_height();

        if( $local_height !== $from )
        {
            $from = min( $local_height, $from );

            if( $local_height > $from )
            {
                w8io_warning( "clear_transactions( $from )" );
                if( !$this->clear_transactions( $from ) )
                    w8io_error( 'unexpected clear_transactions() error' );
                if( false === $this->checkpoint->set_pair( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, $from ) )
                    w8io_error( 'set checkpoint_transactions failed' );
                unset( $this->sponsors );
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

        if( false === $this->checkpoint->set_pair( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, $to ) )
            w8io_error( 'set checkpoint_transactions failed' );

        if( !$this->transactions->commit() )
            w8io_error( 'unexpected commit() error' );

        return [ 'transactions' => $this, 'from' => $from, 'to' => $to ];
    }
}
