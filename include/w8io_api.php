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
    private $pairs_assets_info;
    private $pairs_balances;
    private $pairs_aliases;
    private $pairs_data;

    private function get_aid( $address )
    {
        if( $address === preg_replace( '/[^\-.0123456789@_abcdefghijklmnopqrstuvwxyz\-.]/', '', $address ) )
            return $this->get_pairs_aliases()->get_value( $address );
        else // if( $address === preg_replace( '/[^123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]/', '', $address ) )
            return $this->get_pairs_addresses()->get_id( $address );
    }

    public function get_address_balance( $address )
    {
        $aid = $this->get_aid( $address );
        if( $aid === false )
            return false;

        $balance = $this->get_balances()->get_balance( $aid );
        if( $balance === false )
            return false;

        $balance['balance'] = json_decode( $balance['balance'], true, 512, JSON_BIGINT_AS_STRING );
        return $balance;
    }

    public function get_address_transactions( $address, $height, $limit = 100 )
    {
        $aid = $this->get_aid( $address );
        if( $aid === false )
            return false;

        return $this->get_transactions()->get_txs( $aid, $height, $limit );
    }

    private function get_pairs_aliases()
    {
        if( !isset( $this->pairs_aliases ) )
        {
            require_once 'w8io_pairs.php';
            $this->pairs_aliases = new w8io_pairs( W8IO_DB_BLOCKCHAIN_TRANSACTIONS, 'aliases', false, 'TEXT PRIMARY KEY|INTEGER|0|1' );
        }

        return $this->pairs_aliases;
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
