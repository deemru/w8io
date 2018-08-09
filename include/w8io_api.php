<?php

class w8io_api
{
    private $balances;
    private $transactions;

    private $crypto;
    private $pairs_transactions;
    private $pairs_addresses;
    private $pairs_pubkey_addresses;
    private $pairs_assets;
    private $pairs_asset_info;
    private $pairs_balances;
    private $pairs_aliases;
    private $pairs_data;

    public function get_aid( $address )
    {
        if( strlen( $address ) === 35 )
        {
            if( $address === preg_replace( '/[^123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]/', '', $address ) )
                return $this->get_pairs_addresses()->get_id( $address );
        }
        else if( $address === preg_replace( '/[^\-.0123456789@_abcdefghijklmnopqrstuvwxyz\-.]/', '', $address ) )
            return $this->get_pairs_aliases()->get_value( $address, 'i' );
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

        return false;
    }

    public function get_address( $id )
    {
        return $this->get_pairs_addresses()->get_value( $id );
    }

    public function get_alias( $id )
    {
        return $this->get_pairs_aliases()->get_value( $id );
    }

    public function get_data( $id )
    {
        return $this->get_pairs_data()->get_value( $id );
    }

    public function get_asset_info( $id )
    {
        return $this->get_pairs_asset_info()->get_value( $id, 'j' );
    }

    public function get_asset( $id )
    {
        return $this->get_pairs_assets()->get_id( $id );
    }

    public function get_address_balance( $aid )
    {
        $balance = $this->get_balances()->get_balance( $aid );
        if( $balance === false )
            return false;

        $balance['balance'] = json_decode( $balance['balance'], true, 512, JSON_BIGINT_AS_STRING );
        return $balance;
    }

    public function get_address_transactions( $aid, $height, $limit = 100 )
    {
        if( $aid === false )
            return $this->get_transactions()->get_txs_all( $limit );

        return $this->get_transactions()->get_txs( $aid, $height, $limit );
    }

    public function get_transactions_where( $aid, $where, $limit = 100 )
    {
        return $this->get_transactions()->get_txs_where( $aid, $where, $limit );
    }

    private function get_pairs_aliases()
    {
        if( !isset( $this->pairs_aliases ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_aliases = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'aliases' );
        }

        return $this->pairs_aliases;
    }

    private function get_pairs_data()
    {
        if( !isset( $this->pairs_data ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_data = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'addons' );
        }

        return $this->pairs_data;
    }

    private function get_pairs_addresses()
    {
        if( !isset( $this->pairs_addresses ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_addresses = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'addresses' );
        }

        return $this->pairs_addresses;
    }

    private function get_pairs_asset_info()
    {
        if( !isset( $this->pairs_asset_info ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_asset_info = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'asset_info' );
        }

        return $this->pairs_asset_info;
    }

    private function get_pairs_assets()
    {
        if( !isset( $this->pairs_assets ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_assets = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'assets' );
        }

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
}
