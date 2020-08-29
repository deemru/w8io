<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

class BlockchainBalances
{
    public Triples $balances;
    public KV $uids;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->balances = new Triples( $this->db, 'balances', 1,
            // uid                 | address  | asset    | balance    
            // r0                  | r1       | r2       | r3
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER' ],
            [ 0,                     1,         1,         0 ] );

        $this->balances->db->exec( 'CREATE INDEX IF NOT EXISTS balances_r1_r2_index ON balances( r1, r2 )' );

        $this->uids = new KV;
        $this->setUid();
    }

    public function cacheHalving()
    {
        $this->uids->cacheHalving();
    }

    public function rollback( $pts )
    {
        if( is_array( $pts ) && count( $pts ) > 0 )
            $this->update( $pts, true );
    }

    private function setUid()
    {
        if( false === ( $this->uid = $this->balances->getHigh( 0 ) ) )
            $this->uid = 0;
    }

    private function getNewUid()
    {
        return ++$this->uid;
    }

    public function setParser( $parser )
    {
        $this->parser = $parser;
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

    private function finalizeChanges( $aid, $temp_procs, &$procs )
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

    private function getUid( $address, $asset )
    {
        $key = $address . '_' . $asset;
        $uid = $this->uids->getValueByKey( $key );
        if( $uid !== false )
            return [ $uid, true ];

        if( !isset( $this->q_getUid ) )
        {
            $this->q_getUid = $this->balances->db->prepare( 'SELECT r0 FROM balances WHERE r1 = ? AND r2 = ?' );
            if( $this->q_getUid === false )
                w8io_error( 'getUid' );
        }

        if( false === $this->q_getUid->execute( [ $address, $asset ] ) )
            w8io_error( 'getUid' );

        $uid = $this->q_getUid->fetchAll();
        if( isset( $uid[0] ) )
        {
            $uid = (int)$uid[0][0];
            $update = true;
        }
        else
        {
            $uid = $this->getNewUid();
            $update = false;
        }

        $this->uids->setKeyValue( $key, $uid );
        return [ $uid, $update ];
    }

    private function insertBalance( $uid, $address, $asset, $amount )
    {
        if( !isset( $this->q_insertBalance ) )
        {
            $this->q_insertBalance = $this->balances->db->prepare( 'INSERT INTO balances( r0, r1, r2, r3 ) VALUES( ?, ?, ?, ? )' );
            if( $this->q_insertBalance === false )
                w8io_error( 'insertBalance' );
        }

        if( false === $this->q_insertBalance->execute( [ $uid, $address, $asset, $amount ] ) )
            w8io_error( 'insertBalance' );
    }

    private function updateBalance( $uid, $amount )
    {
        if( !isset( $this->q_updateBalance ) )
        {
            $this->q_updateBalance = $this->balances->db->prepare( 'UPDATE balances SET r3 = r3 + ? WHERE r0 = ?' );
            if( $this->q_updateBalance === false )
                w8io_error( 'updateBalance' );
        }

        if( false === $this->q_updateBalance->execute( [ $amount, $uid ] ) )
            w8io_error( 'updateBalance' );
    }

    private function commitChanges( $procs, $isRollback = false )
    {
        foreach( $procs as $address => $aprocs )
        foreach( $aprocs as $asset => $amount )
        {
            if( $isRollback )
                $amount = -$amount;

            list( $uid, $update ) = $this->getUid( $address, $asset );

            if( $update === false )
                $this->insertBalance( $uid, $address, $asset, $amount );
            else
                $this->updateBalance( $uid, $amount );
        }
    }

    public function processChanges( $ts, &$procs )
    {
        $type = $ts[TYPE];
        $amount = $ts[AMOUNT];
        $asset = $ts[ASSET];
        $fee = $ts[FEE];
        $afee = $ts[FEEASSET];

        //$procs_a = [];
        //$procs_b = [];

        switch( $type )
        {
            case TX_SPONSOR:
                $procs_a = [ $asset => -$amount ];
                $procs_b = [ $asset => +$amount, $afee => -$fee ];
                break;

            case TX_GENERATOR:
                if( $asset === $afee )
                    $procs_a = [ $asset => -$amount -$fee ];
                else
                    $procs_a = [ $asset => -$amount, $afee => -$fee ];
                $procs_b = [ $asset => +$amount ];
                break;
            case TX_GENESIS:
            case TX_PAYMENT:
            case TX_TRANSFER:
            case TX_EXCHANGE:
            case TX_MATCHER:
            case TX_INVOKE:
                if( $asset === $afee )
                    $procs_a = [ $asset => -$amount -$fee ];
                else
                    $procs_a = [ $asset => -$amount, $afee => -$fee ];
                $procs_b = [ $asset => +$amount ];
                break;

            case TX_ISSUE:
            case TX_REISSUE:
                $procs_a = [ $asset => +$amount, $afee => -$fee ];
                break;
            case TX_BURN:
                $procs_a = [ $asset => -$amount, $afee => -$fee ];
                break;

            case TX_LEASE:
                if( w8k2h( $ts[TXKEY] ) > GetHeight_LeaseReset() )
                {
                    $procs_a = [ WAVES_LEASE_ASSET => -$amount, $afee => -$fee ];
                    $procs_b = [ WAVES_LEASE_ASSET => +$amount ];
                }
                else
                {
                    $procs_a = [ $afee => -$fee ];
                }
                break;
            case TX_LEASE_CANCEL:
                if( w8k2h( $ts[TXKEY] ) > GetHeight_LeaseReset() )
                {
                    $procs_a = [ WAVES_LEASE_ASSET => +$amount, $afee => -$fee ];
                    $procs_b = [ WAVES_LEASE_ASSET => -$amount ];
                }
                else
                {
                    $procs_a = [ $afee => -$fee ];
                }
                break;

            case TX_ALIAS:
            case TX_DATA:
            case TX_SMART_ACCOUNT:
            case TX_SPONSORSHIP:
            case TX_SMART_ASSET:
            case TX_UPDATE_ASSET_INFO:
                $procs_a = [ $afee => -$fee ];
                break;

            case TX_MASS_TRANSFER:
                if( $ts[B] === MASS )
                {
                    if( $asset === $afee )
                        $procs_a = [ $asset => -$amount -$fee ];
                    else
                        $procs_a = [ $asset => -$amount, $afee => -$fee ];
                }
                else
                {
                    $procs_b = [ $asset => +$amount ];
                }
                break;

            default:
                w8io_error( 'unknown tx type = ' . $type );
        }

        if( isset( $procs_a ) )
            $this->finalizeChanges( $ts[A], $procs_a, $procs );
        if( isset( $procs_b ) )
            $this->finalizeChanges( $ts[B], $procs_b, $procs );
    }

    public function getAllWaves()
    {
        $balances = $this->balances->db->prepare( 'SELECT r3 FROM balances WHERE r1 > 0 AND r2 = 0' );
        $balances->execute();
        $waves = 0;

        foreach( $balances as $balance )
            $waves += $balance[0];

        return $waves;
    }

    public function update( $pts, $isRollback = false )
    {
        $changes = [];
        foreach( $pts as $ts )
            $this->processChanges( $ts, $changes );
        $this->commitChanges( $changes, $isRollback );
    }
}
