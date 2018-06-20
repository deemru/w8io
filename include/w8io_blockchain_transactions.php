<?php

require_once 'w8io_blockchain.php';

class w8io_blockchain_transactions
{
    private $transactions;
    private $checkpoint;
    
    private $query_get_txs = false;
    private $query_get_txs_asset = false;
    private $query_get_txid = false;
    private $query_set_tx = false;
    private $query_clear = false;
    private $query_last_wtx = false;
    private $query_from_to = false;

    private $crypto;
    private $pairs_txids;
    private $pairs_addresses;
    private $pairs_pubkey_addresses;
    private $pairs_assets;
    private $pairs_asset_info;
    private $pairs_balances;
    private $pairs_aliases;
    private $pairs_data;

    public function __construct( $writable = true )
    {
        $this->transactions = new PDO( 'sqlite:' . W8IO_DB_BLOCKCHAIN_TRANSACTIONS );
        if( !$this->transactions->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING ) )
            w8io_error( 'PDO->setAttribute()' );

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

            $this->transactions->exec( W8IO_DB_PRAGMAS );
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
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_a_fa  ON transactions( a, asset )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_b_fa  ON transactions( b, asset )" );
            $this->transactions->exec( "CREATE INDEX IF NOT EXISTS transactions_index_a_ff  ON transactions( a, afee )" );
        }
    }

    private function get_txid( $txid, $one = false )
    {
        if( $this->query_get_txid == false )
        {
            $this->query_get_txid = $this->transactions->prepare( "SELECT * FROM transactions WHERE txid = :txid" );
            if( !is_object( $this->query_get_txid ) )
                return false;
        }

        if( $this->query_get_txid->execute( array( 'txid' => $txid ) ) === false )
            return false;

        $data = $this->query_get_txid->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $data[0] ) )
            return false;

        if( $one )
        {
            if( count( $data ) != 1 )
                return false;

            return $data[0];
        }

        return $data;
    }

    private function get_last_wtx()
    {
        if( $this->query_last_wtx == false )
        {
            $this->query_last_wtx = $this->transactions->prepare( "SELECT * FROM transactions ORDER BY uid DESC LIMIT 1" );
            if( !is_object( $this->query_last_wtx ) )
                return false;
        }

        if( $this->query_last_wtx->execute() === false )
            return false;

        $data = $this->query_last_wtx->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $data[0] ) )
            return false;

        return $data[0];
    }

    public function get_height()
    {
        $height = $this->checkpoint->get_value( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS );
        if( !$height )
            return 0;
    
        return $height;
    }

    private function clear_transactions( $height )
    {
        if( $this->query_clear === false )
        {
            $this->query_clear = $this->transactions->prepare( 'DELETE FROM transactions WHERE block > :height' );
            if( !is_object( $this->query_clear ) )
                return false;
        }

        if( $this->query_clear->execute( array( 'height' => $height ) ) === false )
            return false;

        return true;
    }

    private function timestamp( $timestamp )
    {
        return (int)substr( $timestamp, 0, -3 );
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
            w8io_error();
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

    public function get_txs( $aid, $height, $limit = 100 )
    {
        if( $this->query_get_txs == false )
        {
            $this->query_get_txs = $this->transactions->prepare( 
                "SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND a = :aid ORDER BY uid DESC LIMIT :limit )
                 UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND b = :aid ORDER BY uid DESC LIMIT :limit ) ORDER BY uid DESC" );
            if( !is_object( $this->query_get_txs ) )
                return false;
        }

        if( $this->query_get_txs->execute( array( 'aid' => $aid, 'height' => $height, 'limit' => $limit ) ) === false )
            return false;

        return $this->query_get_txs;
    }

    public function get_txs_asset( $aid, $height, $asset, $limit = 100 )
    {
        if( $this->query_get_txs_asset == false )
        {
            $this->query_get_txs_asset = $this->transactions->prepare( 
                "SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND a = :aid AND asset = :asset ORDER BY uid DESC LIMIT :limit )
                 UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND a = :aid AND afee = :asset ORDER BY uid DESC LIMIT :limit )
                 UNION
                 SELECT * FROM ( SELECT * FROM transactions WHERE block <= :height AND b = :aid AND asset = :asset ORDER BY uid DESC LIMIT :limit ) ORDER BY uid DESC" );
            if( !is_object( $this->query_get_txs_asset ) )
                return false;
        }

        if( $this->query_get_txs_asset->execute( array( 'aid' => $aid, 'height' => $height, 'asset' => $asset, 'limit' => $limit ) ) === false )
            return false;

        return $this->query_get_txs_asset;
    }

    public function get_from_to( $from, $to )
    {
        if( $this->query_from_to == false )
        {
            $this->query_from_to = $this->transactions->prepare( "SELECT * FROM transactions WHERE block > :from AND block <= :to ORDER BY uid ASC" );
            if( !is_object( $this->query_from_to ) )
                return false;
        }

        if( $this->query_from_to->execute( array( 'from' => $from, 'to' => $to ) ) === false )
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

    private function set_tx( $wtx )
    {
        $a = $wtx['a'];
        if( is_int( $a ) === false )
        {
            if( $a[0] == 'a' )
            {
                var_dump( $wtx );
                w8io_error( 'unexpected alias' );
                //$a = $this->pairs_aliases->get_value( substr( $a, 8 ) );
                //if( $a === false )
                    //return false;
            }
            else if( $a[0] == '3' )
            {
                $a = $this->pairs_addresses->get_id( $wtx['a'], true );
                if( $a === false )
                    return false;
            }
            else if( $a == 'GENESIS' )
            {
                $a = 0;
            }
            else if( $a == 'GENERATOR' )
            {
                $a = -1;
            }
            else if( $a == 'MATCHER' )
            {
                $a = -2;
            }
            else
            {
                var_dump( $wtx );
                w8io_error( 'unexpected $a' );
            }
        }

        $b = $wtx['b'];
        if( is_int( $b ) === false )
        {
            if( $b === false )
            {
                $b = 0;
            }
            else if( $b[0] == 'a' )
            {
                $alias = substr( $b, 8 );

                $b = $this->pairs_aliases->get_value( $alias );
                if( $b === false )
                    return false;

                if( $wtx['data'] !== false )
                    $wtx['data']['b'] = $this->get_dataid( $alias );
                else
                    $wtx['data'] = array( 'b' => $this->get_dataid( $alias ) );
            }
            else if( $b[0] == '3' )
            {
                $b = $this->pairs_addresses->get_id( $wtx['b'], true );
                if( $b === false )
                    return false;
            }
            else if( $b == 'NULL' )
            {
                $b = -3;
            }
            else
            {
                var_dump( $wtx );
                w8io_error( 'unexpected $b' );
            }
        }

        if( $this->query_set_tx === false )
        {
            $this->query_set_tx = $this->transactions->prepare( "INSERT INTO transactions
                (  txid,  block,  type,  timestamp,  a,  b,  amount,  asset,  fee,  afee,  data ) VALUES
                ( :txid, :block, :type, :timestamp, :a, :b, :amount, :asset, :fee, :afee, :data )" );
            if( !is_object( $this->query_set_tx ) )
                return false;
        }

        $wtx['a'] = $a;
        $wtx['b'] = $b;

        if( $wtx['data'] !== false )
            $wtx['data'] = json_encode( $wtx['data'] );

        if( $this->query_set_tx->execute( $wtx ) === false )
            return false;

        $this->last_wtx = $wtx;
        return true;
    }

    private function block_fees( $at, $block, $prev_block )
    {
        $txs = $block['transactions'];
        $fees = array();

        for( $i = 0; $i < 2; $i++ )
        {
            foreach( $txs as $tx )
            {
                if( !empty( $tx['feeAsset'] ) )
                    $asset = $this->get_assetid( $tx['feeAsset'] );
                else
                    $asset = 0;

                $fee = $tx['fee'];

                if( $at >= 805001 )
                {
                    if( $i == 0 )
                        $fee = intdiv( $fee * 2, 5 );
                    else
                        $fee = $fee - intdiv( $fee * 2, 5 );
                }

                if( $fee )
                    $fees[$asset] = $fee + ( isset( $fees[$asset] ) ? $fees[$asset] : 0 );
            }

            if( $i == 1 )
                return $fees;

            if( $at <= 805001 )
                return $fees;

            $txs = $prev_block['transactions'];
        }

        return $fees;
    }

    public function set_fees( $at, $block, $prev_block )
    {
        $fees = $this->block_fees( $at, $block, $prev_block );

        $wtx = array();
        $wtx['txid'] = $this->get_pair_txid( $at, true );
        $wtx['block'] = $at;
        $wtx['type'] = 0;
        $wtx['timestamp'] = $this->timestamp( $block['timestamp'] );
        
        $wtx['fee'] = 0;
        $wtx['afee'] = 0;
        $wtx['data'] = false;

        $wtx['a'] = 'GENERATOR';
        $wtx['b'] = $this->get_aid( $block['generator'], $at == 1 );

        if( count( $fees ) == 0 )
        {
            $wtx['amount'] = 0;
            $wtx['asset'] = 0;

            if( !$this->set_tx( $wtx ) )
                w8io_error();
        }
        else
        foreach( $fees as $asset => $fee )
        {
            $wtx['amount'] = $fee;
            $wtx['asset'] = $asset;

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

    private function set_transactions( $block, $prev_block )
    {
        $at = $block['height'];
        $txs = $block['transactions'];

        foreach( $txs as $tx )
        {
            $type = $tx['type'];
            $wtx = array();
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
                            $wtx['data'] = array( 'd' => $this->get_dataid( $attachment, true ) );
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
                        $ba = $this->pairs_pubkey_addresses->get_value( $pub );
                        if( $ba === false )
                        {
                            $ba = $this->get_crypto()->get_address_from_pubkey( $pub );
                            if( $ba === false )
                                w8io_error();

                            $ba = $this->pairs_addresses->get_id( $ba );
                            if( $ba === false )
                                w8io_error();
                                
                            if( $this->pairs_pubkey_addresses->set_pair( $pub, $ba ) === false )
                                w8io_error();
                        }
                        $ba = intval( $ba );

                        $pub = $seller['senderPublicKey'];
                        $sa = $this->pairs_pubkey_addresses->get_value( $pub );
                        if( $sa === false )
                        {
                            $sa = $this->get_crypto()->get_address_from_pubkey( $pub );
                            if( $sa === false )
                                w8io_error();

                            $sa = $this->get_aid( $sa );

                            if( $this->pairs_pubkey_addresses->set_pair( $pub, $sa ) === false )
                                w8io_error();
                        }
                        $sa = intval( $sa );

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
                    {
                        if( !$this->set_tx( $wtx ) )
                            w8io_error();
                    }
                    $saved = true;
                    break;
                case 9: // cancel lease
                    $wtx['a'] = $tx['sender'];
                    {
                        $txid = $this->get_pair_txid( $tx['leaseId'] );
                        $lease_tx = $this->get_txid( $txid, true );
                        if( $lease_tx === false )
                            w8io_error();

                        $wtx['b'] = intval( $lease_tx['b'] );
                        $wtx['amount'] = $lease_tx['amount'];
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

                        $wtx['data'] = array( 'd' => $this->get_dataid( $alias, true ) );
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

                default:
                    var_dump( $tx );
                    w8io_error( "unexpected tx type = $type ($at)" );
            }

            if( $saved === false && $this->set_tx( $wtx ) === false )
                w8io_error( "set_tx() failed" );
        }

        return $this->set_fees( $at, $block, $prev_block );
    }

    public function update( $upcontext )
    {
        // TODO
        {
            $this->pairs_addresses->set_pair( 0, 'GENESIS' );
            $this->pairs_addresses->set_pair( -1, 'GENERATOR' );
            $this->pairs_addresses->set_pair( -2, 'MATCHER' );
            $this->pairs_addresses->set_pair( -3, 'NULL' );
        }

        $blockchain = $upcontext['blockchain'];
        $from = $upcontext['from'];
        $to = $upcontext['to'];
        $local_height = $this->get_height();

        if( $local_height != $from )
        {
            $from = min( $local_height, $from );
            if( !$this->clear_transactions( $from ) )
                w8io_error( 'unexpected clear_transactions() error' );
        }
        
        $to = min( $to, $from + W8IO_MAX_UPDATE_BATCH );

        if( !$this->transactions->beginTransaction() )
            w8io_error( 'unexpected begin() error' );

        for( $i = $from + 1, $prev_block = false; $i <= $to; $i++ )
        {
            w8io_trace( 'i', "$i (transactions)" );
            
            $block = $blockchain->get_block( $i );
            if( $block === false )
                w8io_error( 'unexpected blockchain->get_block() error' );

            if( $prev_block === false && $i > 805001 )
            {
                $prev_block = $blockchain->get_block( $i - 1 );
                if( $prev_block === false )
                    w8io_error( 'unexpected blockchain->get_block() error' );
            }

            if( !$this->set_transactions( $block, $prev_block ) )
                w8io_error( 'unexpected set_transactions() corruption' );

            $prev_block = $block;
        }

        if( false === $this->checkpoint->set_pair( W8IO_CHECKPOINT_BLOCKCHAIN_TRANSACTIONS, $to ) )
            w8io_error( 'set checkpoint_transactions failed' );

        if( !$this->transactions->commit() )
            w8io_error( 'unexpected commit() error' );

        return array( 'transactions' => $this, 'from' => $from,  'to' => $to );
    }
}
