<?php

use deemru\WavesKit;
use deemru\Pairs;

class w8io_api
{
    private $balances;
    private $transactions;

    private $pairs_addresses;
    private $pairs_assets;
    private $pairs_asset_info;
    private $pairs_aliases;
    private $pairs_data;

    public function get_aid( $address )
    {
        if( strlen( $address ) === 35 )
        {
            if( $address === preg_replace( '/[^123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]/', '', $address ) )
                return $this->get_pairs_addresses()->getKey( $address );
        }
        else if( $address === preg_replace( '/[^\-.0123456789@_abcdefghijklmnopqrstuvwxyz\-.]/', '', $address ) )
            return $this->get_pairs_aliases()->getValue( $address, 'i' );
        else if( $address === 'GENESIS' )
            return 0;
        else if( $address === 'GENERATOR' )
            return -1;
        else if( $address === 'MATCHER' )
            return -2;
        else if( $address === 'NULL' )
            return -3;
        else if( $address === 'SPONSOR' )
            return -4;
        else if( $address === 'MASS' )
            return -5;

        return false;
    }

    private function getBlockPairs()
    {
        if( !isset( $this->pairsBlock ) )
            $this->pairsBlock = new Pairs( W8IO_DB_BLOCKCHAIN, 'blocks' );

        return $this->pairsBlock;
    }

    public function getBlockAt( $height )
    {
        return $this->getBlockPairs()->getValue( $height, 'jz' );
    }

    public function get_address( $id )
    {
        return $this->get_pairs_addresses()->getValue( $id, 's' );
    }

    public function get_alias_id_by_alias( $alias )
    {
        return $this->get_pairs_aliases()->getValue( $alias, 'i' );
    }

    public function get_alias_by_id( $id )
    {
        return $this->get_pairs_aliases()->getKey( $id, false, false );
    }

    public function get_data( $id, $type = 's' )
    {
        return $this->get_pairs_data()->getValue( $id, $type );
    }

    public function get_asset_info( $id )
    {
        return $this->get_pairs_asset_info()->getValue( $id, 'j' );
    }

    public function get_asset( $id )
    {
        return $this->get_pairs_assets()->getKey( $id );
    }

    public function get_asset_id( $key )
    {
        return $this->get_pairs_assets()->getValue( $key, 's' );
    }

    public function get_address_balance( $aid )
    {
        $balance = $this->get_balances()->get_balance( $aid );
        if( $balance === false )
            return false;

        return $balance;
    }

    public function get_asset_distribution( $id )
    {
        $distribution = $this->get_balances()->get_distribution( $id );
        if( $distribution === false )
            return false;
            
        return $distribution;
    }

    public function get_address_transactions( $aid, $height, $limit = 100 )
    {
        if( $aid === false )
            return $this->get_transactions()->get_txs_all( $limit );

        return $this->get_transactions()->get_txs( $aid, $height, $limit );
    }

    public function get_transactions_where( $aid, $where, $uid = false, $limit = 100 )
    {
        return $this->get_transactions()->get_txs_where( $aid, $where, $uid, $limit );
    }

    public function get_transactions_query( $query )
    {
        return $this->get_transactions()->query( $query );
    }

    public function get_all_balances()
    {
        return $this->get_balances()->get_all_waves( true );
    }

    public function get_pairs_aliases()
    {
        if( !isset( $this->pairs_aliases ) )
            $this->pairs_aliases = new Pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'aliases' );

        return $this->pairs_aliases;
    }

    public function get_pairs_data()
    {
        if( !isset( $this->pairs_data ) )
            $this->pairs_data = new Pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'addons' );

        return $this->pairs_data;
    }

    private function get_pairs_addresses()
    {
        if( !isset( $this->pairs_addresses ) )
            $this->pairs_addresses = new Pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'addresses' );

        return $this->pairs_addresses;
    }

    private function get_pairs_asset_info()
    {
        if( !isset( $this->pairs_asset_info ) )
            $this->pairs_asset_info = new Pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'asset_info' );

        return $this->pairs_asset_info;
    }

    private function get_pairs_assets()
    {
        if( !isset( $this->pairs_assets ) )
            $this->pairs_assets = new Pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'assets' );

        return $this->pairs_assets;
    }

    private function get_balances()
    {
        if( !isset( $this->balances ) )
        {
            require_once 'w8io_blockchain_balances.php';
            $this->balances = new w8io_blockchain_balances( false );
        }

        return $this->balances;
    }

    private function get_transactions()
    {
        if( !isset( $this->transactions ) )
        {
            require_once 'w8io_blockchain_transactions.php';
            $this->transactions = new w8io_blockchain_transactions( false );
        }

        return $this->transactions;
    }

    public function get_transactions_id( $id )
    {
        return $this->get_transactions()->get_txid_by_id( $id );
    }

    public function get_incomes( $aid, $from, $to )
    {
        if( $from > $to || $from < 0 || $to < 0 )
            return false;

        $query = $this->get_transactions_query(
            "SELECT * FROM transactions WHERE b = $aid AND type = 8 UNION
             SELECT * FROM transactions WHERE b = $aid AND type = 9 ORDER BY type" );

        $leases = [];
        foreach( $query as $wtx )
        {
            $wtx = w8io_filter_wtx( $wtx );
            $txid = $wtx['txid'];

            if( $wtx['type'] === 8 )
            {
                $start = $wtx['block'];
                if( $start < W8IO_RESET_LEASES )
                    continue;

                $start += 1000;
                if( $start > $to )
                    continue;

                $leases[$txid] = [ 'start' => $start, 'a' => $wtx['a'], 'amount' => $wtx['amount'] ];
            }
            else if( isset( $leases[$txid] ) && $wtx['amount'] )
            {
                $end = $wtx['block'];

                if( $end < $from || $end < $leases[$txid]['start'] )
                {
                    unset( $leases[$txid] );
                    continue;
                }

                $leases[$txid]['end'] = $end;
            }
        }

        $range = $to - $from + 1;
        $total = 0;

        $incomes = [];
        foreach( $leases as $txid => $lease )
        {
            $lease_range = $range;
            if( $lease['start'] > $from )
                $lease_range -= $lease['start'] - $from;
            if( isset( $lease['end'] ) && $to > $lease['end'] )
                $lease_range -= $to - $lease['end'];

            $power = $lease_range / $range * $lease['amount'];
            $total += $power;

            $a = $lease['a'];
            $incomes[$a] = $power + ( isset( $incomes[$a] ) ? $incomes[$a] : 0 );
        }

        foreach( $incomes as $a => $power )
            $incomes[$a] = $power / $total;

        return $incomes;
    }

    public function get_generators( $blocks, $start = null )
    {
        $start = isset( $start ) ? "AND block <= $start" : '';

        $query = $this->get_transactions_query(
            "SELECT * FROM transactions WHERE type = 0 AND asset = 0 $start ORDER BY uid DESC LIMIT $blocks" );

        $generators = [];
        foreach( $query as $wtx )
        {
            $wtx = w8io_filter_wtx( $wtx );
            $generators[$wtx['b']][$wtx['block']] = $wtx;
        }

        return $generators;
    }

    public function correct_balance( $id, $height, $waves_balance = null )
    {
        $waves = 0;
        
        if( isset( $waves_balance ) )
            $height = "block > $height";
        else
            $height = "block <= $height";

        $query = $this->get_transactions_query(
            "SELECT * FROM transactions WHERE a = $id AND asset = 0 AND $height" );

        foreach( $query as $wtx )
        {
            $wtx = w8io_filter_wtx( $wtx );
            switch( $wtx['type'] )
            {
                case W8IO_TYPE_SPONSOR: // sponsor
                case W8IO_TYPE_FEES: // fees
                case 1: // genesis
                case 2: // payment
                case 4: // transfer
                case 7: // exchange
                case 8: // start lease
                    $waves -= $wtx['amount'];
                    break;
                case 9: // cancel lease
                    $waves += $wtx['amount'];
                    break;
                case 11: // mass transfer
                    if( $wtx['b'] < 0 )
                        $waves -= $wtx['amount'];
                    break;
            }
        }

        $query = $this->get_transactions_query(
            "SELECT * FROM transactions WHERE b = $id AND asset = 0 AND $height" );

        foreach( $query as $wtx )
        {
            $wtx = w8io_filter_wtx( $wtx );
            switch( $wtx['type'] )
            {
                case W8IO_TYPE_SPONSOR: // sponsor
                case W8IO_TYPE_FEES: // fees
                case 1: // genesis
                case 2: // payment
                case 4: // transfer
                case 7: // exchange
                case 8: // start lease
                case 11: // mass transfer
                    $waves += $wtx['amount'];
                    break;
                case 9: // cancel lease
                    $waves -= $wtx['amount'];
                    break;
            }
        }

        $query = $this->get_transactions_query(
            "SELECT * FROM transactions WHERE a = $id AND afee = 0 AND $height" );

        foreach( $query as $wtx )
        {
            $wtx = w8io_filter_wtx( $wtx );
            $waves -= $wtx['fee'];
        }

        return isset( $waves_balance ) ? $waves_balance - $waves : $waves;
    }

    private function aggregate_dataset( &$dataset, $height, $json )
    {
        $txs = 0;
        foreach( $json as $type => $value )
        {
            if( !isset( $dataset[$type] ) )
                $dataset[$type] = [];

            $dataset[$type][$height] = $value;

            if( $type !== 0 )
                $txs += $value;

            if( !isset( $dataset['totals'][$type] ) )
                $dataset['totals'][$type] = $value;
            else
                $dataset['totals'][$type] += $value;
        }

        $dataset['txs'][$height] = $txs;
        $dataset['totals']['txs'] += $txs;
    }

    public function get_height()
    {
        $checkpoint = new Pairs( W8IO_DB_BLOCKCHAIN_AGGREGATE, 'checkpoint' );
        return $checkpoint->getValue( W8IO_CHECKPOINT_BLOCKCHAIN_AGGREGATE, 'i' );
    }

    public function get_dataset( $Q, $from, $to )
    {
        $db_name = "db_$Q";
        $db_Q = new Pairs( W8IO_DB_BLOCKCHAIN_AGGREGATE, $db_name );
        $query = $db_Q->query( "SELECT * from $db_name WHERE key > $from AND key <= $to ORDER BY key ASC" );

        if( false === $query )
            return false;

        $dataset = [ 'txs' => [], 'totals' => [ 'txs' => 0 ] ];
        foreach( $query as $rec )
        {
            $height = $rec['key'];
            $json = json_decode( $rec['value'], true, 512, JSON_BIGINT_AS_STRING );
            $this->aggregate_dataset( $dataset, $height, $json );
        }

        $height = $to;
        $json = [];

        while( $to % $Q )
        {
            $from = $to - ( $to % $Q );
            $Q = intdiv( $Q , 10 );
            $from += $Q;

            if( $from > $to )
                continue;

            $db_name = "db_$Q";
            $db_Q = new Pairs( W8IO_DB_BLOCKCHAIN_AGGREGATE, $db_name );

            $json = w8io_aggregate_jsons( $db_Q, $from, $to, $Q, $json );
        }

        $this->aggregate_dataset( $dataset, $height, $json );

        return $dataset;
    }
}
