<?php

use deemru\Pairs;

class w8io_blockchain_balances
{
    public function __construct( $writable = true )
    {
        $this->checkpoint = new Pairs( W8IO_DB_BLOCKCHAIN_BALANCES, 'checkpoint', $writable, 'INTEGER PRIMARY KEY|TEXT|0|0' );
        $this->balances = $this->checkpoint->db();

        if( $writable )
        {
            $this->balances->exec( "CREATE TABLE IF NOT EXISTS balances (
                uid INTEGER PRIMARY KEY AUTOINCREMENT,
                address INTEGER,
                asset INTEGER,
                amount INTEGER )" );
            $this->balances->exec( "CREATE INDEX IF NOT EXISTS balances_index_address ON balances( address )" );
            $this->balances->exec( "CREATE INDEX IF NOT EXISTS balances_index_asset   ON balances( asset )" );
            $this->balances->exec( "CREATE INDEX IF NOT EXISTS balances_index_aa      ON balances( address, asset )" );
        }
    }

    public function get_height()
    {
        $height = $this->checkpoint->getValue( W8IO_CHECKPOINT_BLOCKCHAIN_BALANCES, 'i' );
        if( !$height )
            return 0;

        return $height;
    }

    public function get_balance( $aid )
    {
        if( !isset( $this->query_get_balance ) )
        {
            $this->query_get_balance = $this->checkpoint->db()->prepare( 
                'SELECT value AS k, NULL AS v FROM checkpoint WHERE key = ' . W8IO_CHECKPOINT_BLOCKCHAIN_BALANCES
                . ' UNION ALL SELECT asset, amount FROM balances WHERE address = :aid' );
            if( !is_object( $this->query_get_balance ) )
                return false;
        }

        if( false === $this->query_get_balance->execute( [ 'aid' => $aid ] ) )
            return false;

        $data = $this->query_get_balance->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $data[1] ) )
            return false;

        $balance = [];
        $n = count( $data );
        for( $i = 1; $i < $n; $i++ )
        {
            $kv = $data[$i];
            $balance[(int)$kv['k']] = (int)$kv['v'];
        }

        return [ 'height' => $data[0]['k'], 'balance' => $balance ];
    }

    public function get_distribution( $aid )
    {
        if( !isset( $this->query_get_distribution ) )
        {
            $id = W8IO_CHECKPOINT_BLOCKCHAIN_BALANCES;
            $this->query_get_distribution = $this->checkpoint->db()->prepare( 'SELECT address, amount FROM balances WHERE asset = :aid ORDER BY amount DESC' );
            if( !is_object( $this->query_get_distribution ) )
                return false;
        }

        if( false === $this->query_get_distribution->execute( [ 'aid' => $aid ] ) )
            return false;

        return $this->query_get_distribution;
    }

    private function update_procs( $aid, $temp_procs, &$procs )
    {
        foreach( $temp_procs as $asset => $amount )
        {
            if( $amount === 0 )
                continue;
                
            if( isset( $procs[$aid][$asset] ) )
                $procs[$aid][$asset] += $amount;
            else
                $procs[$aid][$asset] = $amount;
        }
    }

    private function commit_procs( $procs, $rollback = false )
    {
        foreach( $procs as $address => $aprocs )
        foreach( $aprocs as $asset => $amount )
        {
            if( $rollback )
                $amount = -$amount;

            if( !isset( $this->queryBalanceId ) )
            {
                $this->queryBalanceId = $this->balances->prepare( "SELECT uid FROM balances WHERE address = :address AND asset = :asset" );
                if( $this->queryBalanceId === false )
                    return false;
            }

            if( false === $this->queryBalanceId->execute( [ 'address' => $address, 'asset' => $asset ] ) )
                return false;

            $uid = $this->queryBalanceId->fetchAll( \PDO::FETCH_ASSOC );
            $uid = isset( $uid[0]['uid'] ) ? (int)$uid[0]['uid'] : false;

            if( $uid === false )
            // INSERT
            {
                if( !isset( $this->queryBalanceInsert ) )
                {
                    $this->queryBalanceInsert = $this->balances->prepare( "INSERT INTO balances( address, asset, amount ) VALUES( :address, :asset, :amount )" );
                    if( $this->queryBalanceInsert === false )
                        return false;
                }

                if( false === $this->queryBalanceInsert->execute( [ 'address' => $address, 'asset' => $asset, 'amount' => $amount ] ) )
                    return false;
            }
            else
            // UPDATE
            {
                if( !isset( $this->queryBalanceUpdate ) )
                {
                    $this->queryBalanceUpdate = $this->balances->prepare( "UPDATE balances SET amount = amount + :amount WHERE uid = :uid" );
                    if( $this->queryBalanceUpdate === false )
                        return false;
                }

                if( false === $this->queryBalanceUpdate->execute( [ 'amount' => $amount, 'uid' => $uid ] ) )
                    return false;
            }
        }
    }

    public function rollback( $transactions, $from )
    {
        $local_height = $this->get_height();

        if( $local_height > $from )
        // ROLLBACK
        {
            w8io_warning( "balances (rollback to $from)" );

            if( false === ( $wtxs = $transactions->get_from_to( $from, $local_height ) ) )
            w8io_error( 'unexpected get_from_to() error' );
            if( !$this->checkpoint->begin() )
                w8io_error( 'unexpected begin() error' );
            $this->apply_transactions( $wtxs, true );
            if( false === $this->checkpoint->setKeyValue( W8IO_CHECKPOINT_BLOCKCHAIN_BALANCES, $from ) )
                w8io_error( 'set checkpoint_transactions failed' );
            if( !$this->checkpoint->commit() )
                w8io_error( 'unexpected commit() error' );
        }        
    }

    public function update_balances( $wtx, &$procs )
    {
        $procs_a = [];
        $procs_b = [];

        $type = $wtx['type'];

        if( $type === W8IO_TYPE_INVOKE_DATA )
            return;

        $amount = $wtx['amount'];
        $asset = $wtx['asset'];
        $fee = $wtx['fee'];
        $afee = $wtx['afee'];

        switch( $type )
        {
            case W8IO_TYPE_SPONSOR: // sponsor
                if( $asset )
                {
                    $is_a = false;
                    $procs_b[$asset] = +$amount;
                    $is_b = true;
                }
                else
                {
                    $procs_a[0] = -$amount;
                    $is_a = true;
                    $procs_b[0] = +$amount;
                    $is_b = true;
                }
                break;

            case W8IO_TYPE_FEES: // fees
            case 1: // genesis
            case 101: // genesis role
            case 102: // role
            case 110: // genesis unknown
            case 105: // data unknown
            case 106: // invoke 1 unknown
            case 107: // invoke 2 unknown
            case 2: // payment
            case W8IO_TYPE_INVOKE_TRANSFER:
            case 4: // transfer
            case 7: // exchange
            case 16: // invoke
                if( $asset === $afee )
                {
                    $procs_a[$asset] = -$amount -$fee;
                }
                else
                {
                    $procs_a[$asset] = -$amount;
                    $procs_a[$afee] = -$fee;
                }
                $is_a = true;
                $procs_b[$asset] = +$amount;
                $is_b = true;
                break;

            case 3: // issue
            case 5: // reissue
                $procs_a[$asset] = +$amount;
                $procs_a[$afee] = -$fee;
                $is_a = true;
                $is_b = false;
                break;
            case 6: // burn
                $procs_a[$asset] = -$amount;
                $procs_a[$afee] = -$fee;
                $is_a = true;
                $is_b = false;
                break;

            case 8: // start lease
                if( $wtx['block'] > W8IO_RESET_LEASES )
                {
                    $procs_a[W8IO_ASSET_WAVES_LEASED] = -$amount;
                    $procs_a[$afee] = -$fee;
                    $is_a = true;
                    $procs_b[W8IO_ASSET_WAVES_LEASED] = +$amount;
                    $is_b = true;
                }
                else
                {
                    $procs_a[$afee] = -$fee;
                    $is_a = true;
                    $is_b = false;
                }
                break;
            case 9: // cancel lease
                if( $wtx['block'] > W8IO_RESET_LEASES )
                {
                    $procs_a[W8IO_ASSET_WAVES_LEASED] = +$amount;
                    $procs_a[$afee] = -$fee;
                    $is_a = true;
                    $procs_b[W8IO_ASSET_WAVES_LEASED] = -$amount;
                    $is_b = true;
                }
                else
                {
                    $procs_a[$afee] = -$fee;
                    $is_a = true;
                    $is_b = false;
                }
                break;

            case 10: // alias
            case 12: // data
            case 13: // smart account
            case 14: // sponsorship
            case 15: // smart asset
                $procs_a[$afee] = -$fee;
                $is_a = true;
                $is_b = false;
                break;

            case 11: // mass transfer
                if( $wtx['b'] < 0 )
                {
                    if( $asset === $afee )
                    {
                        $procs_a[$asset] = -$amount -$fee;
                    }
                    else
                    {
                        $procs_a[$asset] = -$amount;
                        $procs_a[$afee] = -$fee;
                    }
                    $is_a = true;
                    $procs_b[$afee] = +$fee;
                    $is_b = true;
                }
                else
                {
                    $is_a = false;
                    $procs_b[$asset] = +$amount;
                    $is_b = true;
                }
                break;

            default:
                w8io_error( 'unknown tx type' );
        }

        if( $is_a )
            $this->update_procs( $wtx['a'], $procs_a, $procs );

        if( $is_b ) 
            $this->update_procs( $wtx['b'], $procs_b, $procs );
    }

    public function get_all_waves( $ret = false )
    {
        $balances = $this->checkpoint->db()->prepare( "SELECT amount FROM balances WHERE address > 0 AND asset = 0" );
        $balances->execute();

        if( $ret )
            return $balances;

        $waves = 0;

        foreach( $balances as $balance )
            $waves += $balance[0];

        return $waves;
    }

    private function apply_transactions( $wtxs, $rollback = false )
    {
        $procs = [];
        $i = 0;
        foreach( $wtxs as $wtx )
        {
            if( $i !== $wtx['block'] )
            {
                $i = $wtx['block'];
                w8io_info( "$i (balances)" );
            }

            $this->update_balances( $wtx, $procs );
        }

        if( false === $this->commit_procs( $procs, $rollback ) )
            w8io_error( 'set commit_procs failed' );
    }

    public function update( $upcontext )
    {
        $transactions = $upcontext['transactions'];
        $from = $upcontext['from'];
        $to = $upcontext['to'];
        $local_height = $this->get_height();

        if( $local_height !== $from )
        {
            $from = min( $local_height, $from );

            if( $local_height > $from )
                w8io_error( 'unexpected balances rollback' );
        }

        $to = min( $to, $from + W8IO_MAX_UPDATE_BATCH );
        if( false === ( $wtxs = $transactions->get_from_to( $from, $to ) ) )
            w8io_error( 'unexpected get_from_to() error' );
        if( !$this->checkpoint->begin() )
            w8io_error( 'unexpected begin() error' );
        $this->apply_transactions( $wtxs );
        if( false === $this->checkpoint->setKeyValue( W8IO_CHECKPOINT_BLOCKCHAIN_BALANCES, $to ) )
            w8io_error( 'set checkpoint_transactions failed' );
        if( !$this->checkpoint->commit() )
            w8io_error( 'unexpected commit() error' );

        return [ 'from' => $from, 'to' => $to ];
    }
}
