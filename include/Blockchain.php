<?php

namespace w8io;

use deemru\ABCode;
use deemru\Triples;
use deemru\KV;

define( 'W8IO_STATUS_WARNING', -2 );
define( 'W8IO_STATUS_OFFLINE', -1 );
define( 'W8IO_STATUS_NORMAL', 0 );
define( 'W8IO_STATUS_UPDATED', 1 );

function w8_k2i( $key )
{
    return $key & 0xFFFFFFFF;
}

function w8_k2h( $key )
{
    return $key >> 32;
}

function w8_hi2k( $height, $i = 0 )
{
    return ( $height << 32 ) | $i;
}

function d58( $data )
{
    return ABCode::base58()->decode( $data );
}

function e58( $data )
{
    return ABCode::base58()->encode( $data );
}

function json_unpack( $data )
{
    return json_decode( gzinflate( $data ), true, 512, JSON_BIGINT_AS_STRING );
}

function json_pack( $data )
{
    return gzdeflate( json_encode( $data ), 9 );
}

class Blockchain
{
    public Triples $ts;
    public Triples $hs;
    public BlockchainParser $parser;
    public KV $generators;
    public KV $rewards;

    public function __construct( $db )
    {
        $this->ts = new Triples( $db, 'ts', 1, ['INTEGER PRIMARY KEY', 'INTEGER', 'TEXT'], [0, 1] );
        $this->db = $this->ts;
        $this->hs = new Triples( $this->db, 'hs', 1, ['INTEGER PRIMARY KEY', 'TEXT'] );
        $this->generators = new KV;
        $this->rewards = new KV;

        $this->setHeight();
        $this->setTxHeight();
        $this->lastUp = 0;
    }

    private function setRewardAt( $height, $reward )
    {
        $this->rewards->setKeyValue( $height, $reward );
    }

    private function getRewardAt( $height )
    {
        $reward = $this->rewards->getValueByKey( $height );
        if( $reward === false )
        {
            $header = wk()->getBlockAt( $this->height, true );
            if( $header === false || $this->getMyUniqueAt( $height ) !== $this->blockUnique( $header ) )
                w8io_error( 'getRewardAt' );
            $reward = isset( $header['reward'] ) ? $header['reward'] : 0;
            $this->setRewardAt( $height, $reward );
        }

        return $reward;
    }

    private function setGeneratorAt( $height, $generator )
    {
        $this->generators->setKeyValue( $height, $generator );
    }

    private function getGeneratorAt( $height )
    {
        $generator = $this->generators->getValueByKey( $height );
        if( $generator === false )
        {
            $header = wk()->getBlockAt( $this->height, true );
            if( $header === false || $this->getMyUniqueAt( $height ) !== $this->blockUnique( $header ) )
                w8io_error( 'getGeneratorAt' );
            $generator = $header['generator'];
            $this->setGeneratorAt( $height, $generator );
        }

        return $generator;
    }

    public function setParser( $parser )
    {
        $this->parser = $parser;
        $parser->setBlockchain( $this );
    }

    public function setBalances( $balances )
    {
        $this->balances = $balances;
        $balances->setBlockchain( $this );
    }

    private function tsr( $key, $tx )
    {
        $txid = d58( $tx['id'] );
        $bucket = unpack( 'J1', $txid )[1];
        return [ $key, $bucket, substr( $txid, 8 ) ];
    }

    public function getTransactionId( $key )
    {
        $r = $this->ts->getUno( 0, $key );
        if( $r === false )
            return false;

        return e58( pack( 'J', (int)$r[1] ) . $r[2] );
    }

    public function getTransactionKey( $txid )
    {
        $txid = d58( $txid );
        $bucket = unpack( 'J1', $txid )[1];
        $txpart = substr( $txid, 8 );

        static $q;
        if( !isset( $q ) )
        {
            $q = $this->ts->query( 'SELECT r0 FROM ts WHERE r1 == ? AND r2 == ? ORDER BY r0 DESC LIMIT 1' );
            if( $q === false )
                w8_err();
        }

        if( false === $q->execute( [ $bucket, $txpart ] ) )
            w8_err();

        $r = $q->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    private function setHeight( $height = null )
    {
        if( !isset( $height ) )
        {
            $height = $this->hs->getHigh( 0 );
            if( $height === false )
                $height = 0;
        }

        $this->height = $height;
    }

    private function setTxHeight( $txheight = null )
    {
        if( !isset( $txheight ) )
        {
            $txheight = $this->ts->getHigh( 0 );
            if( $txheight === false )
                $txheight = 0;
        }

        $hi = w8_hi2k( $this->height ) - 1;
        if( $txheight < $hi )
            $txheight = $hi;

        $this->txheight = $txheight;
    }

    public function getMyUniqueAt( $at )
    {
        $q = $this->hs->get( 0, $at )->fetchAll();
        if( !isset( $q[0][1]) )
            return false;

        return e58( $q[0][1] );
    }

    public function selfcheck()
    {
        //return;
        $to = $this->height;
        //$from = $to - 100;
        $from = 402000;
        //$from = 1;

        for( $i = $from; $i <= $to; $i++ )
        {
            $myUnique = $this->getMyUniqueAt( $i );
            $their = wk()->getBlockAt( $i, true );
            if( $myUnique !== $this->blockUnique( $their ) ||
                //$my['reference'] !== $their['reference'] ||
                $their['transactionCount'] !== ( $realCount = count( $txids = $this->getTxIdsAtHeight( $i ) ) ) )
            {
                wk()->log( 'e', 'fail @ ' . $i );
                exit;
            }            
            wk()->log( 'ok @ ' . $i );
        }
        exit;
    }

    public function getTxIdsAtHeight( $height )
    {
        $from = w8_hi2k( $height );
        $to = w8_hi2k( $height + 1 ) - 1;
        return $this->getTxIdsFromTo( $from, $to );
    }

    public function getTxIdsFromTo( $from, $to )
    {
        $query = $this->ts->query( 'SELECT * FROM ts WHERE r0 >= ' . $from . ' AND r0 <= ' . $to . ' ORDER BY r0 ASC' );
        $txids = [];
        foreach( $query as $r )
            $txids[(int)$r[0]] = e58( pack( 'J', (int)$r[1] ) . $r[2] );
        return $txids;
    }

    public function rollback( $from )
    {
        $txfrom = w8_hi2k( $from );
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->hs->query( 'DELETE FROM hs WHERE r0 >= ' . $from );
            $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $txfrom );
        }                    
        $this->db->commit();
        wk()->log( 'i', $this->height . ' >> ' . ( $from - 1 ) . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );

        $this->setHeight( $from - 1 );
        $this->setTxHeight();
    }

    private function fixate( $fixate )
    {
        $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $fixate );

        $height = w8_k2h( $fixate );
        $txfrom = w8_k2i( $this->txheight ) + 1;
        $fixate = w8_k2i( $fixate );
        wk()->log( 'i', $height . ' (' . $txfrom . ' >> ' . $fixate . ') (fixate)' );

        $this->setTxHeight();
    }

    private function blockUnique( $header )
    {
        if( isset( $header['id'] ) )
            return $header['id'];
        return $header['signature'];
    }

    public function update( $block = null )
    {
        //$this->dups();
        //exit;
        $entrance = microtime( true );
        $this->selfcheck();

        if( 0 ) // CUSTOM ROLLBACK
        {
            $this->rollback( 350000 );
            return W8IO_STATUS_UPDATED; 
        }

        if( isset( $block ) )
        {
            $header = $block;
        }
        else if( false === ( $header = wk()->getBlockAt( 0, true ) ) )
        {
            wk()->log( 'w', 'OFFLINE: cannot get last header' );
            return W8IO_STATUS_OFFLINE;
        }

        $height = $header['height'];

        if( 0 === ( $from = $this->height ) )
        {
            $this->height = $i = $from = 1;
            $reference = '67rpwLCuS5DGA8KGZXKsVQ7dnPb9goRLoKfgGbLfQg9WoLUgNY77E2jT11fem3coV9nAkguBACzrU1iyZM4B8roQ';
            wk()->log( 'w', 'starting from GENESIS' );
        }
        else
        for( $i = $from;; )
        {
            if( 0 ) // per block update
            {
                if( $i === $height )
                    return W8IO_STATUS_NORMAL;
            }

            if( !isset( $header ) || $header['height'] !== $i )
            {
                $header = wk()->getBlockAt( $i, true );
                if( $header === false )
                {
                    wk()->log( 'w', 'OFFLINE: cannot get header' );
                    return W8IO_STATUS_OFFLINE;
                }
            }
            
            $blockUnique = $this->getMyUniqueAt( $i );

            // STABLE BLOCK
            if( $blockUnique === $this->blockUnique( $header ) )
            {
                wk()->log( 'd', 'stable @ ' . $i );
                if( $i == $height )
                    return W8IO_STATUS_NORMAL;

                $reference = $blockUnique;
                $from = ++$i;

                if( $this->height >= $from )
                {
                    $rollback = true;
                    $fixate = w8_hi2k( $from );
                }
                
                break;
            }
            
            // BLOCK UPDATE
            if( $i === $from )
            {
                $reference = $this->getMyUniqueAt( $i - 1 );
                if( $reference === $header['reference'] )
                {
                    wk()->log( 'd', 'update @ ' . $i );
                    $update = true;
                    break;
                }
            }

            wk()->log( 'w', 'fork @ ' . $i );
            $i--;
        }

        $newHdrs = [];
        $newTxs = [];

        $to = min( $height, $from + W8IO_MAX_UPDATE_BATCH - 1 );
        //$to = min( $height, $from + 1 - 1 );
        for( ; $i <= $to; $i++ )
        {
            if( !isset( $block ) || $block['height'] !== $i )
            {
                $block = wk()->getBlockAt( $i );
                if( $block === false )
                {
                    wk()->log( 'w', 'OFFLINE: cannot get block' );
                    return W8IO_STATUS_OFFLINE;
                }
            }            

            if( $reference !== $block['reference'] )
            {
                wk()->log( 'w', 'on-the-fly change @ ' . $i );
                return W8IO_STATUS_WARNING;
            }

            if( isset( $header ) )
            {
                if( $header['height'] === $i && $header['signature'] !== $block['signature'] )
                {
                    wk()->log( 'd', 'repeat update @ ' . $i );
                    return $this->update( $block );
                }
                unset( $header );
            }

            $n = $block['transactionCount'];
            if( $n )
            {
                $txs = $block['transactions'];
                $key = w8_hi2k( $i );
                for( $j = 0; $j < $n; ++$j, ++$key )
                {
                    $tx = $txs[$j];
                    $txid = $tx['id'];
                    
                    if( isset( $update ) )
                    {
                        if( !isset( $txids ) )
                            $txids = $this->getTxIdsAtHeight( $i );
                        if( isset( $txids[$key] ) && $txids[$key] === $txid )
                            continue;
                        $fixate = $key;
                        unset( $update );
                    }

                    if( $i >= StatusHeight() )
                    {
                        $tx = wk()->getTransactionById( $txid );
                        if( !isset( $tx['applicationStatus'] ) )
                            w8_err();
                    }
                    
                    $type = $tx['type'];
                    if( $type === 16 )
                    {
                        $tx = wk()->getStateChanges( $txid );
                        if( $tx === false || $tx['height'] !== $i )
                        {
                            wk()->log( 'i', 'OFFLINE: cannot get state changes' );
                            return W8IO_STATUS_OFFLINE;
                        }
                    }

                    $newTxs[$key] = $tx;
                    $txheight = $key;
                }
            }

            if( !isset( $fixate ) )
            {
                $fixate = w8_hi2k( $i, $n );
                unset( $update );
            }

            unset( $block['transactions'] );
            $reference = $this->blockUnique( $block );
            $newHdrs[$i] = $block;

            if( $from < $i )
                wk()->log( ( $i - 1 ) . ' (' . $lastCount . ')' );
            
            $lastCount = $n;
        }

        if( 0 === count( $newHdrs ) )
            return W8IO_STATUS_NORMAL;

        $this->db->begin();
        {
            if( isset( $rollback ) )
                $this->rollback( $from );
            else if( $this->txheight >= $fixate )
                $this->fixate( $fixate );

            $hs = [];
            foreach( $newHdrs as $height => $block )
            {
                $this->setGeneratorAt( $height, $block['generator'] );
                $this->setRewardAt( $height, isset( $block['reward'] ) ? $block['reward'] : 0 );
                $hs[] = [ $height, d58( $this->blockUnique( $block ) ) ];
            }
            $this->hs->merge( $hs );

            if( count( $newTxs ) )
            {
                $ts = [];
                foreach( $newTxs as $key => $tx )
                    $ts[] = $this->tsr( $key, $tx );
                $this->ts->merge( $ts );
            }

            $parserTxs = $newTxs;
            for( $height = $this->height; $height < $to; ++$height )
            {
                $generator = $this->getGeneratorAt( $height, true );
                $reward = $this->getRewardAt( $height, true );
                $parserTxs[w8_hi2k( $height + 1 ) - 1] = [ 'type' => TX_GENERATOR, 'generator' => $generator, 'reward' => $reward ];
            }

            if( count( $parserTxs ) )
            {
                ksort( $parserTxs );
                $this->parser->update( $parserTxs );

                // SELFTEST
                if( 0 && count( $newTxs ) === 0 )
                {
                    $waves = $this->balances->getAllWaves();
                    $ngfees = $this->parser->getNGFeesAt( $to - 1 );
                    if( isset( $ngfees[0] ) )
                        $waves += $ngfees[0];
                    $cmp = 10000000000000000;
                    if( $to > 97100 )
                        $cmp += 600000000 * ( $to - 97100 );
                    if( $waves !== $cmp )
                        w8_err();
                }
            }

            $this->setHeight( $height );
            if( count( $newTxs ) )
                $this->setTxHeight( $txheight );
            else
                $this->setTxHeight( $this->txheight );
        }            
        $this->db->commit();

        $this->lastUp = microtime( true );
        $txs = w8_k2h( $this->txheight ) == $this->height ? ( w8_k2i( $this->txheight ) + 1 ) : '0';
        $newTxs = $from === $to ? ( ' +' . count( $newTxs ) ) : '';  
        wk()->log( 's', $to . ' (' . $txs . ')' . $newTxs . ' (' . (int)( ( $this->lastUp - $entrance ) * 1000 ) . ' ms)' );
        return W8IO_STATUS_UPDATED;
    }
}

if( !isset( $lock ) )
    require_once '../w8io_updater.php';
