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

    private $getTxKeyByTxId;

    public function getTxKeyByTxId( $txid )
    {
        $txid = d58( $txid );
        if( $txid === false )
            return false;
        $bucket = unpack( 'J1', $txid )[1];
        $txpart = substr( $txid, 8 );

        if( !isset( $this->getTxKeyByTxId ) )
        {
            $this->getTxKeyByTxId = $this->db->db->prepare( 'SELECT r0 FROM ts WHERE r1 = ? AND r2 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getTxKeyByTxId === false )
                w8_err();
        }

        if( false === $this->getTxKeyByTxId->execute( [ $bucket, $txpart ] ) )
            w8_err();

        $r = $this->getTxKeyByTxId->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    private $getTxIdByTxKey;

    public function getTxIdByTxKey( $txkey )
    {
        if( !isset( $this->getTxIdByTxKey ) )
        {
            $this->getTxIdByTxKey = $this->db->db->prepare( 'SELECT r1, r2 FROM ts WHERE r0 = ? LIMIT 1' );
            if( $this->getTxIdByTxKey === false )
                w8_err();
        }

        if( false === $this->getTxIdByTxKey->execute( [ $txkey ] ) )
            w8_err();

        $r = $this->getTxIdByTxKey->fetchAll();
        if( isset( $r[0] ) )
            return e58( pack( 'J', (int)$r[0][0] ) . $r[0][1] );

        return false;
    }

    private $getKVsByAddress;

    public function getKVsByAddress( $aid )
    {
        if( !isset( $this->getKVsByAddress ) )
        {
            $this->getKVsByAddress = $this->db->db->prepare( 'SELECT * FROM data WHERE r3 = ? AND r2 = 1 ORDER BY r0 DESC LIMIT 1000' );
            if( $this->getKVsByAddress === false )
                w8_err();
        }

        if( false === $this->getKVsByAddress->execute( [ $aid ] ) )
            w8_err();

        return $this->getKVsByAddress;
    }

    private $getValueByKeyAddress;

    public function getValueByAddressKey( $aid, $kid )
    {
        if( !isset( $this->getValueByKeyAddress ) )
        {
            $this->getValueByKeyAddress = $this->db->db->prepare( 'SELECT * FROM data WHERE r3 = ? AND r4 = ? ORDER BY r0 DESC LIMIT 1' );
            if( $this->getValueByKeyAddress === false )
                w8_err();
        }

        if( false === $this->getValueByKeyAddress->execute( [ $aid, $kid ] ) )
            w8_err();

        $value = $this->getValueByKeyAddress->fetchAll();
        return $value[0] ?? false;
    }

    private $getIdByKey;

    public function getIdByKey( $key )
    {
        if( !isset( $this->getIdByKey ) )
        {
            $this->getIdByKey = $this->db->db->prepare( 'SELECT r0 FROM datakeys WHERE r1 = ?' );
            if( $this->getIdByKey === false )
                w8_err();
        }

        if( false === $this->getIdByKey->execute( [ $key ] ) )
            w8_err();

        $id = $this->getIdByKey->fetchAll();
        if( isset( $id[0] ) )
            return $id[0][0];
        return false;
    }

    private $getKeyById;

    public function getKeyById( $id )
    {
        if( !isset( $this->getKeyById ) )
        {
            $this->getKeyById = $this->db->db->prepare( 'SELECT r1 FROM datakeys WHERE r0 = ?' );
            if( $this->getKeyById === false )
                w8_err();
        }

        if( false === $this->getKeyById->execute( [ $id ] ) )
            w8_err();

        return $this->getKeyById->fetchAll()[0][0];
    }

    private $getValueById;

    public function getValueByTypeId( $type, $id )
    {
        if( $type === TYPE_INTEGER )
            return $id;
        else
        if( $type === TYPE_BOOLEAN )
            return $id === 1 ? true : false;
        else
        if( $type === TYPE_NULL )
            return null;

        if( !isset( $this->getValueById ) )
        {
            $this->getValueById = $this->db->db->prepare( 'SELECT r1 FROM datavalues WHERE r0 = ?' );
            if( $this->getValueById === false )
                w8_err();
        }

        if( false === $this->getValueById->execute( [ $id ] ) )
            w8_err();

        $value = $this->getValueById->fetchAll()[0][0];

        if( $type === TYPE_STRING )
            return $value;
        else
        if( $type === TYPE_BINARY )
            return 'base64:' . base64_encode( $value );

        w8_err( 'unknown type ' . $type );
    }

    public function getAddressIdByString( $string )
    {
        switch( $string )
        {
            case 'GENESIS': return GENESIS;
            case 'GENERATOR': return GENERATOR;
            case 'MATCHER': return MATCHER;
            case 'SELF': return MYSELF;
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

    public function getGenerators( $blocks, $start = null )
    {
        if( !isset( $start ) )
            $start = $this->getLastHeightTimestamp()[0] - $blocks;

        $query = $this->db->db->prepare( 'SELECT * FROM pts WHERE r1 = ?' );
        if( $query !== false )
        {
            $generators = [];
            for( $i = 0; $i < $blocks; ++$i )
            {
                $height = $start + $i;
                $query->execute( [ w8h2kg( $height ) ] );
                foreach( $query as $ts )
                {
                    if( $ts[A] !== -1 )
                        continue;
                    $generator = $ts[B];
                    $generators[$generator][$height] = $ts[AMOUNT] + ( $generators[$generator][$height] ?? 0 );
                }
            }

            return $generators;
        }
    }

    private $getTimestampByHeight;

    public function getTimestampByHeight( $height )
    {
        if( !isset( $this->getTimestampByHeight ) )
        {
            $this->getTimestampByHeight = $this->db->db->prepare( 'SELECT r2 FROM hs WHERE r0 = ?' );
            if( $this->getTimestampByHeight === false )
                w8_err();
        }

        if( false === $this->getTimestampByHeight->execute( [ $height ] ) )
            w8_err();

        $r = $this->getTimestampByHeight->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    public function getLastHeightTimestamp()
    {
        $r = $this->db->db->query( 'SELECT * FROM hs ORDER BY r0 DESC LIMIT 1' )->fetchAll();
        if( isset( $r[0] ) )
            return [ (int)$r[0][0], (int)$r[0][2] ];

        return false;
    }

    private $q_getAddressIdByAddress;

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

    private $q_getAddressIdByAlias;

    public function getAddressIdByAlias( $alias )
    {
        if( !isset( $this->q_getAddressIdByAlias ) )
        {
            $this->q_getAddressIdByAlias = $this->db->db->prepare( 'SELECT r1 FROM aliasInfo WHERE r0 IN ( SELECT r0 FROM aliases WHERE r1 = ? LIMIT 1 ) LIMIT 1' );
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

    private $q_getAddressById;

    public function getAddressById( $id )
    {
        switch( $id )
        {
            case GENESIS: return 'GENESIS';
            case GENERATOR: return 'GENERATOR';
            case MATCHER: return 'MATCHER';
            case MYSELF: return 'SELF';
            case SPONSOR: return 'SPONSOR';
            case MASS: return 'MASS';
            default:
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
        }
    }

    private $getFirstAliasById;

    public function getFirstAliasById( $id )
    {
        if( !isset( $this->getFirstAliasById ) )
        {
            $this->getFirstAliasById = $this->db->db->prepare( 'SELECT r1 FROM aliases WHERE r0 = ( SELECT r9 FROM pts WHERE r3 = ? AND r2 = 10 ORDER BY r0 DESC LIMIT 1 ) LIMIT 1' );
            if( $this->getFirstAliasById === false )
                w8_err();
        }

        if( false === $this->getFirstAliasById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getFirstAliasById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getAliasById;

    public function getAliasById( $id )
    {
        if( !isset( $this->getAliasById ) )
        {
            $this->getAliasById = $this->db->db->prepare( 'SELECT r1 FROM aliases WHERE r0 = ? LIMIT 1' );
            if( $this->getAliasById === false )
                w8_err();
        }

        if( false === $this->getAliasById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getAliasById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getGroupById;

    public function getGroupById( $id )
    {
        if( !isset( $this->getGroupById ) )
        {
            $this->getGroupById = $this->db->db->prepare( 'SELECT r1 FROM groups WHERE r0 = ? LIMIT 1' );
            if( $this->getGroupById === false )
                w8_err();
        }

        if( false === $this->getGroupById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getGroupById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getGroupByName;

    public function getGroupByName( $name )
    {
        if( !isset( $this->getGroupByName ) )
        {
            $this->getGroupByName = $this->db->db->prepare( 'SELECT r0 FROM groups WHERE r1 = ? LIMIT 1' );
            if( $this->getGroupByName === false )
                w8_err();
        }

        if( false === $this->getGroupByName->execute( [ $name ] ) )
            w8_err();

        $r = $this->getGroupByName->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $q_getBalanceByAddressId;

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

    private $getAssetInfoById;

    public function getAssetInfoById( $id )
    {
        if( $id === 0 )
            return '8_Waves';

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

    private $getFunctionById;

    public function getFunctionById( $id )
    {
        if( !isset( $this->getFunctionById ) )
        {
            $this->getFunctionById = $this->db->db->prepare( 'SELECT r1 FROM functions WHERE r0 = ?' );
            if( $this->getFunctionById === false )
                w8_err();
        }

        if( false === $this->getFunctionById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getFunctionById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getFunctionByName;

    public function getFunctionByName( $name )
    {
        if( !isset( $this->getFunctionByName ) )
        {
            $this->getFunctionByName = $this->db->db->prepare( 'SELECT r0 FROM functions WHERE r1 = ?' );
            if( $this->getFunctionByName === false )
                w8_err();
        }

        if( false === $this->getFunctionByName->execute( [ $name ] ) )
            w8_err();

        $r = $this->getFunctionByName->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getAssetById;

    public function getAssetById( $id )
    {
        if( !isset( $this->getAssetById ) )
        {
            $this->getAssetById = $this->db->db->prepare( 'SELECT r1 FROM assets WHERE r0 = ?' );
            if( $this->getAssetById === false )
                w8_err();
        }

        if( false === $this->getAssetById->execute( [ $id ] ) )
            w8_err();

        $r = $this->getAssetById->fetchAll();
        if( isset( $r[0] ) )
            return $r[0][0];

        return false;
    }

    private $getIdByAsset;

    public function getIdByAsset( $asset )
    {
        if( !isset( $this->getIdByAsset ) )
        {
            $this->getIdByAsset = $this->db->db->prepare( 'SELECT r0 FROM assets WHERE r1 = ?' );
            if( $this->getIdByAsset === false )
                w8_err();
        }

        if( false === $this->getIdByAsset->execute( [ $asset ] ) )
            w8_err();

        $r = $this->getIdByAsset->fetchAll();
        if( isset( $r[0] ) )
            return (int)$r[0][0];

        return false;
    }

    public function getPTSByAddressId( $id, $filter, $limit, $uid, $d )
    {
        if( $filter === 'io' )
        {
            $query = 'SELECT * FROM (';
            $prolog = ' SELECT * FROM ( SELECT * FROM pts WHERE ' . ( $d === 1 ? 'r4' : 'r3' ) . ' = ' . $id . ' AND r2 = ';
            $epilog = ( $uid !== false ? ' AND r0 <= ' . $uid : '' ) . ' ORDER BY r0 DESC LIMIT ' . $limit . ' )';
            for( $i = TX_GENESIS; $i < TX_ETHEREUM; ++$i )
            {
                if( $i !== 1 )
                    $query .= ' UNION';
                $query .= $prolog . $i . $epilog;
            }
            $query .= ' ) ORDER BY r0 DESC LIMIT ' . $limit;

            return $this->db->query( $query )->fetchAll();
        }

        $wheres = [];
        if( $filter !== false )
            $wheres[] = $filter;
        if( $uid !== false )
            $wheres[] = 'r0 <= ' . $uid;

        $where = '';
        $n = count( $wheres );
        if( $n > 0 )
        {
            for( $i = 0; $i < $n; ++$i )
                if( $i === 0 )
                    $where = ( $id === false ? ' WHERE ' : ' AND ' ) . $wheres[$i];
                else
                    $where .= ' AND ' . $wheres[$i] . ' ';
        }

        if( $id === false )
            $query = 'SELECT * FROM pts ' . $where . ' ORDER BY r0 DESC LIMIT ' . $limit;
        else if( $d === 1 )
            $query = 'SELECT * FROM pts WHERE r4 = ' . $id . $where . ' ORDER BY r0 DESC LIMIT ' . $limit;
        else if( $d === 2 )
            $query = 'SELECT * FROM pts WHERE r3 = ' . $id . $where . ' ORDER BY r0 DESC LIMIT ' . $limit;
        else
            $query = '
SELECT * FROM (
    SELECT * FROM ( SELECT * FROM pts WHERE r3 = ' . $id . $where . ' ORDER BY r0 DESC LIMIT ' . $limit . ' )
    UNION
    SELECT * FROM ( SELECT * FROM pts WHERE r4 = ' . $id . $where . ' ORDER BY r0 DESC LIMIT ' . $limit . ' )
) ORDER BY r0 DESC LIMIT ' . $limit;

        return $this->db->query( $query )->fetchAll();
    }

    private $getPTSAtHeight;

    public function getPTSAtHeight( $height )
    {
        if( !isset( $this->getPTSAtHeight ) )
        {
            $this->getPTSAtHeight = $this->db->db->prepare( 'SELECT * FROM pts WHERE r1 >= ? AND r1 <= ?' );
            if( $this->getPTSAtHeight === false )
                w8_err();
        }

        if( false === $this->getPTSAtHeight->execute( [ w8h2k( $height ), w8h2kg( $height ) ] ) )
            w8_err();

        $pts = $this->getPTSAtHeight->fetchAll();
        if( isset( $pts[0] ) )
            return $pts;

        return false;
    }

    public function getLeasingIncomes( $aid, $from, $to )
    {
        if( $from > $to || $from < 0 || $to < 0 )
            return false;

        require_once 'w8_update_helpers.php';
        $leases = [];

        $query = $this->db->query( "SELECT * FROM pts WHERE r4 = $aid AND r2 = 8" );
        foreach( $query as $ts )
        {
            $amount = (int)$ts[AMOUNT];
            if( $amount === 0 )
                continue;

            $txkey = (int)$ts[TXKEY];
            $a = (int)$ts[A];

            $start = w8k2h( $txkey );
            if( $start < GetHeight_LeaseReset() )
                continue;

            $start += 1000;
            if( $start > $to )
                continue;

            $leases[$txkey] = [ 'start' => $start, 'a' => $a, 'amount' => $amount ];
        }

        $query = $this->db->query( "SELECT * FROM pts WHERE r4 = $aid AND r2 = 9" );
        foreach( $query as $ts )
        {
            $txkey = (int)$ts[TXKEY];
            $ltxkey = (int)$ts[ADDON];
            if( isset( $leases[$ltxkey] ) )
            {
                $end = w8k2h( $txkey );

                if( $end < $from || $end < $leases[$ltxkey]['start'] )
                {
                    unset( $leases[$ltxkey] );
                    continue;
                }

                $leases[$ltxkey]['end'] = $end;
            }
        }

        $range = $to - $from + 1;
        $total = 0;

        $incomes = [];
        foreach( $leases as $lease )
        {
            $lease_range = $range;
            if( $lease['start'] > $from )
                $lease_range -= $lease['start'] - $from;
            if( isset( $lease['end'] ) && $to > $lease['end'] )
                $lease_range -= $to - $lease['end'];

            $power = $lease_range / $range * $lease['amount'];
            $total += $power;

            $a = $lease['a'];
            $incomes[$a] = $power + ( isset( $incomes[$a] ) ? $incomes[$a] : 0 );
        }

        foreach( $incomes as $a => $power )
            $incomes[$a] = $power / $total;

        return $incomes;
    }
}
