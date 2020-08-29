<?php

namespace w8io;

require_once 'common.php';

use deemru\Triples;

class RO
{
    public Triples $db;

    public function __construct( $db )
    {
        $this->db = new Triples( $db, 'pts' );
    }

    public function getTxKeyById( $txid )
    {
        $txid = d58( $txid );
        $bucket = unpack( 'J1', $txid )[1];
        $txpart = substr( $txid, 8 );

        if( !isset( $this->q_getTxKeyById ) )
        {
            $this->q_getTxKeyById = $this->db->db->prepare( 'SELECT r0 FROM ts WHERE r1 = ? AND r2 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->q_getTxKeyById === false )
                w8_err();
        }

        if( false === $this->q_getTxKeyById->execute( [ $bucket, $txpart ] ) )
            w8_err();

        $r = $this->q_getTxKeyById->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    public function getAddressId( $string )
    {
        switch( $string )
        {
            case 'GENESIS': return GENESIS;
            case 'GENERATOR': return GENERATOR;
            case 'MATCHER': return MATCHER;
            case 'UNDEFINED': return UNDEFINED;
            case 'SPONSOR': return SPONSOR;
            case 'MASS': return MASS;
            default:
            {
                if( strlen( $string ) === 35 )
                {
                    if( $string === preg_replace( '/[^123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz]/', '', $string ) )
                        return $this->getAddressIdByAddress( $string );
                }
                else if( $string === preg_replace( '/[^\-.0123456789@_abcdefghijklmnopqrstuvwxyz\-.]/', '', $string ) )
                    return $this->getAddressIdByAlias( $string );

                return false;
            }
        }
    }

    public function getAddressIdByAddress( $address )
    {
        if( !isset( $this->q_getAddressIdByAddress ) )
        {
            $this->q_getAddressIdByAddress = $this->db->db->prepare( 'SELECT r0 FROM addresses WHERE r1 = ?' );
            if( $this->q_getAddressIdByAddress === false )
                w8_err();
        }

        if( false === $this->q_getAddressIdByAddress->execute( [ $address ] ) )
            w8_err();

        $r = $this->q_getAddressIdByAddress->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    public function getAddressIdByAlias( $alias )
    {
        if( !isset( $this->q_getAddressIdByAlias ) )
        {
            $this->q_getAddressIdByAlias = $this->db->db->prepare( 'SELECT r1 FROM aliases WHERE r0 = ?' );
            if( $this->q_getAddressIdByAlias === false )
                w8_err();
        }

        if( false === $this->q_getAddressIdByAlias->execute( [ $alias ] ) )
            w8_err();

        $r = $this->q_getAddressIdByAlias->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    public function getAddressById( $id )
    {
        if( !isset( $this->q_getAddressById ) )
        {
            $this->q_getAddressById = $this->db->db->prepare( 'SELECT r1 FROM addresses WHERE r0 = ?' );
            if( $this->q_getAddressById === false )
                w8_err();
        }

        if( false === $this->q_getAddressById->execute( [ $id ] ) )
            w8_err();

        $r = $this->q_getAddressById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    public function getBalanceByAddressId( $id )
    {
        if( !isset( $this->q_getBalanceByAddressId ) )
        {
            $this->q_getBalanceByAddressId = $this->db->db->prepare( 'SELECT r2, r3 FROM balances WHERE r1 = ?' );
            if( $this->q_getBalanceByAddressId === false )
                w8_err();
        }

        if( false === $this->q_getBalanceByAddressId->execute( [ $id ] ) )
            w8_err();

        $rs = $this->q_getBalanceByAddressId->fetchAll();
        if( !isset( $rs[0] ) )
            return false;

        foreach( $rs as $r )
            $balance[(int)$r[0]] = (int)$r[1];

        return $balance;
    }

    public function getAssetInfoById( $id )
    {
        if( !isset( $this->getAssetInfoById ) )
        {
            $this->getAssetInfoById = $this->db->db->prepare( 'SELECT r1 FROM assetInfo WHERE r0 = ?' );
            if( $this->getAssetInfoById === false )
                w8_err();
        }

        if( false === $this->getAssetInfoById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getAssetInfoById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    public function getPTSByAddressId( $id )
    {
        if( !isset( $this->getPTSByAddressId ) )
        {
            //SELECT * FROM ( SELECT * FROM transactions WHERE a = $aid$where ORDER BY uid DESC LIMIT $limit ) UNION
                 //SELECT * FROM ( SELECT * FROM transactions WHERE b = $aid$where ORDER BY uid DESC LIMIT $limit ) ORDER BY uid DESC LIMIT $limit

            $this->getPTSByAddressId = $this->db->db->prepare( 'SELECT * FROM ( SELECT * FROM pts WHERE r3 = ? ORDER BY r0 DESC LIMIT 100 )
                                                                UNION
                                                                SELECT * FROM pts WHERE r4 = ? ORDER BY r0 DESC LIMIT 100' );
            if( $this->getPTSByAddressId === false )
                w8_err();
        }

        if( false === $this->getPTSByAddressId->execute( [ $id, $id ] ) )
            w8_err();

        return $this->getPTSByAddressId->fetchAll();
    }

    public function getPTSAtHeight( $height )
    {
        if( !isset( $this->getPTSAtHeight ) )
        {
            $this->getPTSAtHeight = $this->db->db->prepare( 'SELECT * FROM pts WHERE r1 >= ? AND r1 < ? ORDER BY r0 DESC LIMIT 1000' );
            if( $this->getPTSAtHeight === false )
                w8_err();
        }

        if( false === $this->getPTSAtHeight->execute( [ w8h2k( $height ), w8h2k( $height + 1 ) ] ) )
            w8_err();

        $rs = $this->getPTSAtHeight->fetchAll();
        if( isset( $rs[0] ) )
            return $rs;

        return false;
    }
}
