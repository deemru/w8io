<?php

namespace w8io;

use deemru\Triples;
use deemru\KV;

define( 'W8IO_STATUS_WARNING', -2 );
define( 'W8IO_STATUS_OFFLINE', -1 );
define( 'W8IO_STATUS_NORMAL', 0 );
define( 'W8IO_STATUS_UPDATED', 1 );

class Blockchain
{
    public Triples $ts;
    public Triples $hs;
    public BlockchainParser $parser;

    private Triples $db;
    private $lastUp;
    private $height;
    private $txheight;
    private $lastBlock;
    private $q_getTxIdsFromTo;

    public function __construct( $db )
    {
        $this->ts = new Triples( $db, 'ts', 1, ['INTEGER PRIMARY KEY', 'INTEGER', 'TEXT'], [0, 1] );

        $this->db = $this->ts;
        $this->db->db->exec( 'PRAGMA journal_size_limit = ' . ( 8 * 1024 * 1024 ) );
        $this->db->db->exec( 'PRAGMA wal_autocheckpoint = ' . ( 8 * 1024 ) );

        $this->hs = new Triples( $this->db, 'hs', 1, ['INTEGER PRIMARY KEY', 'TEXT', 'INTEGER'] );
        $this->parser = new BlockchainParser( $this->db );

        $this->setHeight();
        $this->setTxHeight();
        $this->lastUp = 0;
    }

    private function ts2r( $key, $tx )
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

        return e58( pack( 'J', $r[1] ) . $r[2] );
    }

    public function height()
    {
        return $this->height;
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

        $hi = w8h2kg( $this->height - 1);
        if( $txheight < $hi )
            $txheight = $hi;

        $this->txheight = $txheight;
    }

    public function getMyUniqueAt( $at )
    {
        $q = $this->hs->getUno( 0, $at );
        if( !isset( $q[1]) )
            return false;

        return e58( $q[1] );
    }

    public function selfcheck()
    {
        return;
        $height = 2210000;
        $hs = [];
        for( $i = 1; $i <= $height; ++$i )
        {
            $block = wk()->getBlockAt( $i, true );
            $hs[] = [ $i, d58( $this->blockUnique( $block ) ), intdiv( $block['timestamp'], 1000 ) ];
            if( $i % 10000 === 0 )
                $this->hs->merge( $hs );
        }
        $this->hs->merge( $hs );
        exit( 'ok');
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
        $from = w8h2k( $height );
        $to = w8h2kg( $height );
        return $this->getTxIdsFromTo( $from, $to );
    }

    public function getTxIdsFromTo( $from, $to )
    {
        if( !isset( $this->q_getTxIdsFromTo ) )
        {
            $this->q_getTxIdsFromTo = $this->ts->db->prepare( 'SELECT * FROM ts WHERE r0 >= ? AND r0 <= ? ORDER BY r0 ASC' );
            if( $this->q_getTxIdsFromTo === false )
                w8_err();
        }

        if( false === $this->q_getTxIdsFromTo->execute( [ $from, $to ] ) )
            w8_err();

        $txids = [];
        foreach( $this->q_getTxIdsFromTo as $r )
            $txids[$r[0]] = e58( pack( 'J', $r[1] ) . $r[2] );

        return $txids;
    }

    public function rollback( $from )
    {
        if( $this->height - $from > 1000 )
            $this->rollback( $from + 1000 );

        $txfrom = w8h2k( $from ) - 1; // all txs + last generator
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->parser->rollback( $txfrom );
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
        $this->parser->rollback( $fixate );
        $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $fixate );

        $height = w8k2h( $fixate );
        $txfrom = w8k2i( $this->txheight ) + 1;
        $fixate = w8k2i( $fixate );
        wk()->log( 'i', $height . ' (' . $txfrom . ' >> ' . $fixate . ') (fixate)' );

        $this->setTxHeight();
    }

    private function blockUnique( $header )
    {
        return $header['id'] ?? $header['signature'];
    }

    public function update( $block = null )
    {
        $entrance = microtime( true );

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

            if( ( $header['height'] ?? 0 ) !== $i )
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
                    $fixate = w8h2k( $from );
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
        $txCount = 0;

        $to = min( $height, $from + W8IO_MAX_UPDATE_BATCH - 1 );
        //$to = min( $height, $from + 1 - 1 );
        for( ; $i <= $to; $i++ )
        {
            if( ( $block['height'] ?? 0 ) !== $i )
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
                $key = w8h2k( $i );
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

                    if( in_array( $tx['type'], [ TX_INVOKE, TX_EXPRESSION, TX_ETHEREUM ] ) )
                    {
                        $tx = wk()->getTransactionById( $txid );
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
                $fixate = w8h2k( $i, $n );
                unset( $update );
            }

            unset( $block['transactions'] );
            $reference = $this->blockUnique( $block );
            $newHdrs[$i] = $block;

            if( 0 && $i > $from )
            {
                wk()->log( ( $i - 1 ) . ' (' . $txCount . ')' );
                $txCount = 0;
            }
            
            $txCount += $n;
        }

        if( 0 === count( $newHdrs ) )
            return W8IO_STATUS_NORMAL;

        $this->db->begin();
        {
            if( isset( $rollback ) )
                $this->rollback( $from );
            else if( $this->txheight >= $fixate )
            {
                $this->fixate( $fixate );
                $txCount -= w8k2i( $fixate );
            }

            $hs = [];
            foreach( $newHdrs as $height => $block )
                $hs[] = [ $height, d58( $this->blockUnique( $block ) ), intdiv( $block['timestamp'], 1000 ) ];
            $this->hs->merge( $hs );

            if( count( $newTxs ) )
            {
                $ts = [];
                foreach( $newTxs as $key => $tx )
                    $ts[] = $this->ts2r( $key, $tx );
                $this->ts->merge( $ts );
            }

            $parserTxs = $newTxs;
            for( $height = $this->height; $height < $to; ++$height )
            {
                if( isset( $newHdrs[$height] ) )
                    $block = $newHdrs[$height];
                else if( isset( $this->lastBlock ) )
                    $block = $this->lastBlock;
                if( !isset( $block['height'] ) || $block['height'] !== $height )
                {
                    $block = wk()->getBlockAt( $this->height, true );
                    if( !isset( $block['height'] ) || $block['height'] !== $height )
                        w8_err( 'unexpected block @ ' . $height );
                }

                $generator = $block['generator'];
                $reward = $block['reward'] ?? 0;
                $rewardShares = $block['rewardShares'] ?? [ $generator => $reward ];
                $parserTxs[w8h2kg( $height )] = [ 'type' => TX_GENERATOR, 'generator' => $generator, 'reward' => $reward, 'rewardShares' => $rewardShares ];
            }

            if( count( $parserTxs ) )
            {
                ksort( $parserTxs );
                $this->parser->update( $parserTxs );
            }

            $this->setHeight( $height );
            if( count( $newTxs ) )
                $this->setTxHeight( $txheight );
            else
                $this->setTxHeight( $this->txheight );
            
            $this->lastBlock = $newHdrs[$height];
        }            
        $this->db->commit();

        $this->lastUp = microtime( true );
        $newTxs = $from === $to ? ( ' +' . count( $newTxs ) ) : '';
        $ram = memory_get_usage( true ) / 1024 / 1024;
        $ram = sprintf( '%.00f MiB', $ram );
        wk()->log( 's', $to . ' (' . $txCount . ')' . $newTxs . ' (' . (int)( ( $this->lastUp - $entrance ) * 1000 ) . ' ms) (' . $ram . ')' );
        return W8IO_STATUS_UPDATED;
    }
}
