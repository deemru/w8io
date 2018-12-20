<?php

require_once 'w8io_pairs.php';
require_once 'w8io_blockchain_transactions.php';

class w8io_blockchain_aggregate
{
    private $aggregate;
    private $checkpoint;
    private $db_1;
    private $db_10;
    private $db_100;
    private $db_1000;
    private $db_10000;

    public function __construct( $writable = true )
    {
        $this->aggregate = new PDO( 'sqlite:' . W8IO_DB_BLOCKCHAIN_AGGREGATE );
        if( !$this->aggregate->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING ) || false === $this->aggregate->exec( W8IO_DB_PRAGMAS ) )
            w8io_error( 'PDO->setAttribute() || PDO->exec( pragmas )' );

        if( $writable )
        {
            $this->checkpoint = new w8io_pairs( $this->aggregate, 'checkpoint', $writable, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->db_1 = new w8io_pairs( $this->aggregate, 'db_1', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->db_10 = new w8io_pairs( $this->aggregate, 'db_10', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->db_100 = new w8io_pairs( $this->aggregate, 'db_100', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->db_1000 = new w8io_pairs( $this->aggregate, 'db_1000', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
            $this->db_10000 = new w8io_pairs( $this->aggregate, 'db_10000', true, 'INTEGER PRIMARY KEY|TEXT|0|0' );
        }
    }

    public function get_height()
    {
        $height = $this->checkpoint->get_value( W8IO_CHECKPOINT_BLOCKCHAIN_AGGREGATE, 'i' );
        if( !$height )
            return 0;

        return $height;
    }

    private function aggregate_wtxs( $wtxs )
    {
        $sum = [];

        foreach( $wtxs as $wtx )
        {
            $type = $wtx['type'];

            if( $type === 0 )
            {
                if( $wtx['asset'] !== 0 )
                    continue;

                if( isset( $sum[$type] ) )
                    $sum[$type] += $wtx['amount'];
                else
                    $sum[$type] = $wtx['amount'];
                
                continue;
            }

            if( $type === 11 ) // mass 101 >> 100
            {
                if( $wtx['b'] < 0 ) 
                    continue;
            }
            else if( $type === 7 ) // exchange 3 >> 1
            {
                if( $wtx['a'] > 0 )
                    continue;
            }
            else if( $type === -1 ) // sponsor 2 >> 1
            {
                if( $wtx['a'] > 0 )
                    continue;
            }

            if( isset( $sum[$type] ) )
                $sum[$type]++;
            else
                $sum[$type] = 1;
        }

        return $sum;
    }

    private function aggregate_db( $db_Q, $db_N, $height, $Q, $count )
    {
        $data = w8io_aggregate_jsons( $db_Q, $height - $Q * ( $count - 1 ), $height, $Q );
        if( false === $db_N->set_pair( $height, $data, 'j' ) )
            w8io_error( 'set_pair() failed' );
    }

    private function aggregate( $height, $wtxs )
    {
        $data = $this->aggregate_wtxs( $wtxs );
        if( false === $this->db_1->set_pair( $height, $data, 'j' ) )
            w8io_error( 'set_pair() failed' );

        if( $height % 10 === 0 )
            $this->aggregate_db( $this->db_1, $this->db_10, $height, 1, 10 );
        if( $height % 100 === 0 )
            $this->aggregate_db( $this->db_10, $this->db_100, $height, 10, 10 );
        if( $height % 1000 === 0 )
            $this->aggregate_db( $this->db_100, $this->db_1000, $height, 100, 10 );
        if( $height % 10000 === 0 )
            $this->aggregate_db( $this->db_1000, $this->db_10000, $height, 1000, 10 );
    }

    public function update( $upcontext )
    {
        $transactions = $upcontext['transactions'];
        $from = $upcontext['from'];
        $to = $upcontext['to'];
        $local_height = $this->get_height();

        if( $local_height !== $from )
        {
            if( $local_height > $from )
            {
                $backup = W8IO_DB_BLOCKCHAIN_AGGREGATE . '.backup';
                if( file_exists( $backup ) )
                {
                    unset( $this->checkpoint );
                    unset( $this->db_1 );
                    unset( $this->db_10 );
                    unset( $this->db_100 );
                    unset( $this->db_1000 );
                    unset( $this->db_10000 );
                    unset( $this->aggregate );

                    $backup_diff = md5_file( $backup ) !== md5_file( W8IO_DB_BLOCKCHAIN_AGGREGATE );

                    if( $backup_diff )
                    {
                        unlink( W8IO_DB_BLOCKCHAIN_AGGREGATE );
                        copy( $backup, W8IO_DB_BLOCKCHAIN_AGGREGATE );
                        chmod( W8IO_DB_BLOCKCHAIN_AGGREGATE, 0666 );
                        if( file_exists( W8IO_DB_BLOCKCHAIN_AGGREGATE . '-shm' ) )
                            chmod( W8IO_DB_BLOCKCHAIN_AGGREGATE . '-shm', 0666 );
                        if( file_exists( W8IO_DB_BLOCKCHAIN_AGGREGATE . '-wal' ) )
                            chmod( W8IO_DB_BLOCKCHAIN_AGGREGATE . '-wal', 0666 );
                    }

                    $this->__construct();

                    if( $backup_diff )
                    {
                        w8io_warning( 'restoring from backup (aggregate)' );
                        return $this->update( $upcontext );
                    }
                }

                w8io_warning( 'full reset (aggregate)' );
                $local_height = 0;

                if( false === $this->checkpoint->set_pair( W8IO_CHECKPOINT_BLOCKCHAIN_AGGREGATE, $local_height ) )
                    w8io_error( 'set checkpoint_transactions failed' );
            }

            $from = min( $local_height, $from );
        }

        $to = min( $to, $from + W8IO_MAX_UPDATE_BATCH );

        if( !$this->aggregate->beginTransaction() )
            w8io_error( 'unexpected begin() error' );

        for( $i = $from + 1; $i <= $to; $i++ )
        {
            w8io_trace( 'i', "$i (aggregate)" );

            $wtxs = $transactions->get_from_to( $i - 1, $i );
            if( $wtxs === false )
                w8io_error( 'unexpected get_from_to() error' );

            $this->aggregate( $i, $wtxs );
        }

        if( false === $this->checkpoint->set_pair( W8IO_CHECKPOINT_BLOCKCHAIN_AGGREGATE, $to ) )
            w8io_error( 'set checkpoint_transactions failed' );

        if( !$this->aggregate->commit() )
            w8io_error( 'unexpected commit() error' );

        if( $to % 10000 === 0 )
        {
            $copy = W8IO_DB_BLOCKCHAIN_AGGREGATE . '.copy';
            $backup = W8IO_DB_BLOCKCHAIN_AGGREGATE . '.backup';

            if( file_exists( $copy ) )
            {
                if( file_exists( $backup ) )
                    unlink( $backup );

                rename( $copy, $backup );
            }

            unset( $this->checkpoint );
            unset( $this->db_1 );
            unset( $this->db_10 );
            unset( $this->db_100 );
            unset( $this->db_1000 );
            unset( $this->db_10000 );
            unset( $this->aggregate );

            copy( W8IO_DB_BLOCKCHAIN_AGGREGATE, $copy );
            $this->__construct();
        }

        return [ 'from' => $from, 'to' => $to ];
    }
}
