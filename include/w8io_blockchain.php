<?php

namespace w8io;

require_once 'Markers.php';
require_once "KV2.php";

use deemru\WavesKit;
use deemru\ABCode;
use deemru\Pairs;
use deemru\Triples;
use deemru\KV;
use w8io\Markers;

define( 'W8IO_TXSHIFT', 100000000 );
define( 'W8IO_STATUS_WARNING', -2 );
define( 'W8IO_STATUS_OFFLINE', -1 );
define( 'W8IO_STATUS_NORMAL', 0 );
define( 'W8IO_STATUS_UPDATED', 1 );

function d58( $data )
{
    return ABCode::base58()->decode( $data );
}

function ddata( $data )
{
    return json_decode( gzinflate( $data ), true, 512, JSON_BIGINT_AS_STRING );
}

function edata( $data )
{
    return gzdeflate( json_encode( $data ), 9 );
}

function bucket( $data )
{
    if( $data === '5jpwaJnERa8Gr1ChgrNYnmxm2EtZ4KHC5bW1ZLL7LCY1bUV9gFWFAGjpJaPDCawmFzguqGBgYDyeocpEsKWeYDM1' )
    {
        $data = d58( $data );
        
        return unpack( 'J1', $data )[1];
    }
    return unpack( 'J1', d58( $data ) )[1];
}

class Blockchain
{
    public Markers $markers;

    public function __construct( $path )
    {
        $s = 'S:/w8io-refresh/raw.sqlite3';
        $this->markers = new Markers( $s );
        $this->db = $this->markers->db();
        $this->ts = new Triples( $this->db , 'ts', 1, ['INTEGER PRIMARY KEY', 'INTEGER', 'BLOB'], [0, 1] );
        $this->hs = new Triples( $this->db, 'hs', 1, ['INTEGER PRIMARY KEY', 'BLOB'] );

        $this->setHeight();
        $this->setTxHeight();
        $this->lastUp = 0;
    }

    public function getTransactionByTxHeight( $r0 )
    {
        $tx = $this->ts->getUno( 0, $r0 );
        if( $tx === false )
            return false;

        return ddata( $tx[2] );
    }

    public function getTransactionsById( $id, $uno = false )
    {
        $q = $this->ts->get( 1, bucket( $id ) );
        $max = 0;
        $txs = [];
        foreach( $q as $r )
        {
            $tx = ddata( $r[2] );
            if( $id !== $tx['id'] )
                continue;

            if( $uno )
            {
                $key = (int)$r[0];
                if( $max < $key )
                {
                    $max = $key;
                    $txUno = $tx;
                }
            }

            $txs[] = $tx;
        }

        if( $uno )
            return isset( $txUno ) ? $txUno : false;

        return isset( $txs[0] ) ? $txs : false;
    }

    public function setHeight()
    {
        $height = $this->hs->getHigh( 0 );
        $this->height = $height === false ? 0 : $height;
    }

    private function setTxHeight()
    {
        $txheight = $this->ts->getHigh( 0 );
        $this->txheight = $txheight === false ? 0 : $txheight;
    }

    public function getHeaderAt( $at )
    {
        $q = $this->hs->get( 0, $at )->fetchAll();
        if( !isset( $q[0][1]) )
            return false;

        return ddata( $q[0][1] );
    }

    public function selfcheck()
    {
        return;
        $to = $this->height;
        //$from = $to - 100;
        $from = 2006100;
        //$from = 1;

        for( $i = $from; $i <= $to; $i++ )
        {
            $my = $this->getHeaderAt( $i );
            $their = wk()->getBlockAt( $i, true );
            if( $my['signature'] !== $their['signature'] ||
                $my['reference'] !== $their['reference'] ||
                $my['transactionCount'] !== $their['transactionCount'] ||
                $my['transactionCount'] !== ( $realCount = count( $txids = $this->getTransactionsAtHeight( $i ) ) ) )
            {
                wk()->log( 'e', 'fail @ ' . $i );
                exit;
            }            
            wk()->log( 'ok @ ' . $i );
        }
        exit;
    }

    public function dups()
    {
        $data = d58( '5jpwaJnERa8Gr1ChgrNYnmxm2EtZ4KHC5bW1ZLL7LCY1bUV9gFWFAGjpJaPDCawmFzguqGBgYDyeocpEsKWeYDM1' );
        //$q = $this->ts->query( 'SELECT r0, COUNT(r2) FROM ts GROUP BY r1 HAVING COUNT(*) > 1' );
        //$q = $this->ts->query( 'SELECT r0, COUNT(r2) FROM ts GROUP BY r1 HAVING COUNT(*) > 1' );
        $q = $this->ts->query( 'SELECT r0 FROM ts GROUP BY r1 HAVING COUNT(*) > 1' );
        foreach( $q as $r )
        {
            $tx = $this->tx( $r[0] );
            wk()->log( $r[0] );
            $txs = $this->getTransactionsById( $tx['id'] );
            if( count( $txs ) > 1 && $txs[0]['type'] !== 10 )
            {
                foreach( $txs as $tx )
                wk()->log( $tx['id'] );
            }
        }
    }

    public function getTransactionsAtHeight( $height )
    {
        $from = $height * W8IO_TXSHIFT;
        $to = $from + W8IO_TXSHIFT - 1;
        return $this->getTransactionsFromTo( $from, $to );
    }

    public function getTransactionsFromTo( $from, $to )
    {
        $query = $this->ts->query( 'SELECT * FROM ts WHERE r0 >= ' . $from . ' AND r0 <= ' . $to . ' ORDER BY r0 ASC' );
        $txs = [];
        foreach( $query as $r )
            $txs[(int)$r[0]] = ddata( $r[2] );
        return $txs;
    }

    public function rollback( $from )
    {
        $txfrom = $from * W8IO_TXSHIFT;
        $tt = microtime( true );
        $this->db->begin();
        {
            $this->hs->query( 'DELETE FROM hs WHERE r0 >= ' . $from );
            $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $txfrom );
            $this->markers->setMarkers( $txfrom, $txfrom );
        }                    
        $this->db->commit();
        wk()->log( 'i', $this->height . ' >> ' . ( $from - 1 ) . ' (rollback) (' . (int)( 1000 * ( microtime( true ) - $tt ) ) . ' ms)' );

        $this->height = $from - 1;
        $this->setTxHeight();
    }

    private function fixate( $fixate )
    {
        $this->ts->query( 'DELETE FROM ts WHERE r0 >= ' . $fixate );
        $this->markers->setMarkers( $fixate, $fixate );
                    
        $height = intdiv( $fixate, W8IO_TXSHIFT );
        $txfrom = ( $this->txheight % W8IO_TXSHIFT ) + 1;
        $fixate = $fixate % W8IO_TXSHIFT;
        wk()->log( 'i', $height . ' (' . $txfrom . ' >> ' . $fixate . ') (fixate)' );

        $this->setTxHeight();
    }

    public function update( $block = null )
    {
        //$this->dups();
        //exit;
        $entrance = microtime( true );
        $this->selfcheck();

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
            $i = $from = 1;
            $reference = '67rpwLCuS5DGA8KGZXKsVQ7dnPb9goRLoKfgGbLfQg9WoLUgNY77E2jT11fem3coV9nAkguBACzrU1iyZM4B8roQ';
            wk()->log( 'w', 'starting from GENESIS' );
        }
        else
        for( $i = $from;; )
        {
            if( 10 ) // per block update
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
            
            $my = $this->getHeaderAt( $i );
            $signature = $my['signature'];

            // STABLE BLOCK
            if( $signature === $header['signature'] )
            {
                wk()->log( 'd', 'stable @ ' . $i );
                if( $i == $height )
                    return W8IO_STATUS_NORMAL;

                $reference = $signature;
                $from = ++$i;

                if( $this->height >= $from )
                {
                    $rollback = true;
                    $fixate = $from * W8IO_TXSHIFT;
                }
                
                break;
            }
            
            // BLOCK UPDATE
            if( $i === $from )
            {
                $reference = $my['reference'];
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

        if( 0 ) // CUSTOM ROLLBACK
        {
            $this->rollback( 1880000 );
            return W8IO_STATUS_UPDATED; 
        }

        $newHdrs = [];
        $newTxs = [];

        $to = min( $height, $from + W8IO_MAX_UPDATE_BATCH - 1 );
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
                $key = $i * W8IO_TXSHIFT;
                for( $j = 0; $j < $n; ++$j, ++$key )
                {
                    $tx = $txs[$j];
                    $txid = $tx['id'];
                    
                    if( isset( $update ) )
                    {
                        if( !isset( $txids ) )
                            $txids = $this->getTransactionsAtHeight( $i );
                        if( isset( $txids[$key] ) && $txid === $txids[$key]['id'] )
                            continue;
                        $fixate = $key;
                        unset( $update );
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

                    $newTxs[] = [ $key, bucket( $txid ), edata( $tx ) ];
                    $txheight = $key;
                }
            }

            if( !isset( $fixate ) )
            {
                $fixate = $i * W8IO_TXSHIFT + $n;
                unset( $update );
            }

            unset( $block['transactions'] );
            $reference = $block['signature'];
            $newHdrs[] = [ $i, edata( $block ) ];

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
            
            $this->hs->merge( $newHdrs );
            $this->height = $to;

            if( count( $newTxs ) )
            {
                $this->ts->merge( $newTxs );
                $this->txheight = $txheight;
                $hi = $txheight;
                if( $this->txheight >= $to * W8IO_TXSHIFT )
                    $txs = ( $this->txheight % W8IO_TXSHIFT ) + 1;
                else
                    $txs = 0;
            }
            else
            {
                $hi = ( $to * W8IO_TXSHIFT ) - 1;
                if( $this->txheight > $hi )
                    $hi = $this->txheight;
                else
                    $this->txheight = $hi;
                $txs = 0;
            }

            $this->markers->setMarkers( null, $hi );
        }            
        $this->db->commit();

        $this->lastUp = microtime( true );
        $newTxs = $from === $to ? ( ' +' . count( $newTxs ) ) : '';  
        wk()->log( 's', $to . ' (' . $txs . ')' . $newTxs . ' (' . (int)( ( $this->lastUp - $entrance ) * 1000 ) . ' ms)' );
        return W8IO_STATUS_UPDATED;
    }
}

if( !isset( $lock ) )
    require_once '../w8io_updater.php';
