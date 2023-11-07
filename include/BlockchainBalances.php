<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

class BlockchainBalances
{
    public Triples $balances;
    public KV $uids;

    private Triples $db;
    private $uid;
    private $parser;
    private $empty;

    public function __construct( $db )
    {
        $this->db = $db;
        $this->balances = new Triples( $this->db, 'balances', 1,
            // uid                 | address  | asset    | balance
            // r0                  | r1       | r2       | r3
            [ 'INTEGER PRIMARY KEY', 'INTEGER', 'INTEGER', 'INTEGER' ],
//          [ 0,                     1,         1,         0 ] );
            [ 0,                     0,         0,         0 ] );

        $indexer =
        [
            'CREATE INDEX IF NOT EXISTS balances_r1_index ON balances( r1 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_index ON balances( r2 )',
            'CREATE INDEX IF NOT EXISTS balances_r2_r3_index ON balances( r2, r3 )',
            'CREATE INDEX IF NOT EXISTS balances_r1_r2_index ON balances( r1, r2 )',
        ];

        $this->uids = new KV;
        $this->setUid();
        $this->empty = $this->uid === 0;
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

    private function finalizeChanges( $aid, $temp_procs, &$procs )
    {
        foreach( $temp_procs as $asset => $amount )
        {
            if( $amount === 0 )
                continue;

            $procs[$aid][$asset] = $amount + ( $procs[$aid][$asset] ?? 0 );
        }
    }

    private $q_getUid;

    private function getUid( $address, $asset )
    {
        $key = $address . '_' . $asset;
        $uid = $this->uids->getValueByKey( $key );
        if( $uid !== false )
            return [ $uid, true ];

        if( !$this->empty )
        {
            if( !isset( $this->q_getUid ) )
            {
                $this->q_getUid = $this->balances->db->prepare( 'SELECT r0 FROM balances WHERE r1 = ? AND r2 = ?' );
                if( $this->q_getUid === false )
                    w8_err( 'getUid' );
            }

            if( false === $this->q_getUid->execute( [ $address, $asset ] ) )
                w8_err( 'getUid' );

            $uid = $this->q_getUid->fetchAll();
        }

        if( isset( $uid[0] ) )
        {
            $uid = $uid[0][0];
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

    private $q_insertBalance;

    private function insertBalance( $uid, $address, $asset, $amount )
    {
        if( !isset( $this->q_insertBalance ) )
        {
            $this->q_insertBalance = $this->balances->db->prepare( 'INSERT INTO balances( r0, r1, r2, r3 ) VALUES( ?, ?, ?, ? )' );
            if( $this->q_insertBalance === false )
                w8_err( 'insertBalance' );
        }

        if( false === $this->q_insertBalance->execute( [ $uid, $address, $asset, $amount ] ) )
            w8_err( 'insertBalance' );
    }

    private $q_updateBalance;

    private function updateBalance( $uid, $amount )
    {
        if( !isset( $this->q_updateBalance ) )
        {
            $this->q_updateBalance = $this->balances->db->prepare( 'UPDATE balances SET r3 = r3 + ? WHERE r0 = ?' );
            if( $this->q_updateBalance === false )
                w8_err( 'updateBalance' );
        }

        if( false === $this->q_updateBalance->execute( [ $amount, $uid ] ) )
            w8_err( 'updateBalance' );
    }

    private function commitChanges( $procs, $isRollback = false )
    {
        foreach( $procs as $address => $aprocs )
        foreach( $aprocs as $asset => $amount )
        {
            if( $isRollback )
                $amount = -$amount;

            [ $uid, $update ] = $this->getUid( $address, $asset );

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

        switch( $type )
        {
            case TX_SPONSOR:
                $procs_a = [ asset_out( $type ) => 1, $asset => -$amount ];
                $procs_b = [ asset_in( $type ) => 1, $asset => +$amount, $afee => -$fee ];
                break;

            case TX_GENERATOR:
                if( $asset === $afee )
                    $procs_a = [ asset_out( $type ) => 1, $asset => -$amount -$fee ];
                else
                    $procs_a = [ asset_out( $type ) => 1, $asset => -$amount, $afee => -$fee ];
                $procs_b = [ asset_in( $type ) => 1, $asset => +$amount ];
                break;

            case TX_GENESIS:
            case TX_PAYMENT:
            case TX_TRANSFER:
            case ITX_TRANSFER:
            case TX_EXCHANGE:
            case TX_MATCHER:
            case TX_INVOKE:
            case ITX_INVOKE:
            case TX_ETHEREUM:
            case TX_REWARD:
                if( $asset === $afee )
                    $procs_a = [ asset_out( $type ) => 1, $asset => -$amount -$fee ];
                else
                    $procs_a = [ asset_out( $type ) => 1, $asset => -$amount, $afee => -$fee ];
                $procs_b = [ asset_in( $type ) => 1, $asset => +$amount ];
                break;

            case TX_ISSUE:
            case ITX_ISSUE:
            case TX_REISSUE:
            case ITX_REISSUE:
                $procs_a = [ asset_out( $type ) => 1, $asset => +$amount, $afee => -$fee ];
                break;

            case TX_BURN:
            case ITX_BURN:
                $procs_a = [ asset_out( $type ) => 1, $asset => -$amount, $afee => -$fee ];
                break;

            case TX_LEASE:
            case ITX_LEASE:
                if( w8k2h( $ts[TXKEY] ) > GetHeight_LeaseReset() )
                {
                    $procs_a = [ asset_out( $type ) => 1, WAVES_LEASE_ASSET => -$amount, $afee => -$fee ];
                    $procs_b = [ asset_in( $type ) => 1, WAVES_LEASE_ASSET => +$amount ];
                }
                else
                {
                    $procs_a = [ asset_out( $type ) => 1, $afee => -$fee ];
                }
                break;

            case TX_LEASE_CANCEL:
            case ITX_LEASE_CANCEL:
                if( w8k2h( $ts[TXKEY] ) > GetHeight_LeaseReset() )
                {
                    $procs_a = [ asset_out( $type ) => 1, WAVES_LEASE_ASSET => +$amount, $afee => -$fee ];
                    $procs_b = [ asset_in( $type ) => 1, WAVES_LEASE_ASSET => -$amount ];
                }
                else
                {
                    $procs_a = [ asset_out( $type ) => 1, $afee => -$fee ];
                }
                break;

            case TX_ALIAS:
            case TX_DATA:
            case TX_SMART_ACCOUNT:
            case TX_SPONSORSHIP:
            case ITX_SPONSORSHIP:
            case TX_SMART_ASSET:
            case TX_UPDATE_ASSET_INFO:
            case TX_EXPRESSION:
                $procs_a = [ asset_out( $type ) => 1, $afee => -$fee ];
                break;

            case TX_MASS_TRANSFER:
                if( $ts[B] === MASS )
                {
                    if( $asset === $afee )
                        $procs_a = [ asset_out( $type ) => 1, $asset => -$amount -$fee ];
                    else
                        $procs_a = [ asset_out( $type ) => 1, $asset => -$amount, $afee => -$fee ];
                }
                else
                {
                    return $this->finalizeChanges( $ts[B], [ asset_in( $type ) => 1, $asset => +$amount ], $procs );
                }
                break;

            default:
                w8_err( 'unknown tx type = ' . $type );
        }

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
