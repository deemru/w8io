<?php

class w8io_pairs
{
    private $db;
    private $name;
    private $cache_size;
    private $cache_by_id;
    private $cache_by_value;

    private $query_get_id;
    private $query_get_value;
    private $query_set_value;
    private $query_set_pair;

    public function __construct( $db, $name, $writable = false, $type = 'INTEGER PRIMARY KEY|TEXT UNIQUE|0|0', $cache_size = W8IO_CACHE_PAIRS )
    {
        if( is_a( $db, 'PDO' ) )
        {
            $this->db = $db;
        }
        else
        {
            $this->db = new PDO( "sqlite:$db" );
            if( !$this->db->setAttribute( PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING ) )
                w8io_error( 'PDO->setAttribute()' );

            $this->db->exec( W8IO_DB_PRAGMAS );
        }

        $this->name = $name;
        $this->cache_size = $cache_size;
        $this->cache_by_id = [];

        if( $writable )
        {
            $this->db->exec( W8IO_DB_WRITE_PRAGMAS );

            $type = explode( '|', $type );

            $this->db->exec( "CREATE TABLE IF NOT EXISTS {$this->name}( id {$type[0]}, value {$type[1]} )" );

            if( $type[2] )
                $this->db->exec( "CREATE INDEX IF NOT EXISTS {$this->name}_id_index ON {$this->name}( id )" );

            if( $type[3] )
                $this->db->exec( "CREATE INDEX IF NOT EXISTS {$this->name}_value_index ON {$this->name}( value )" );

            if( $type[3] || false !== strpos( $type[1], 'UNIQUE' ) )
                $this->cache_by_value = [];
        }
    }

    public function reset()
    {
        $this->db->exec( "DELETE FROM {$this->name}" );
        $this->reset_cache();
    }

    public function get_db()
    {
        return $this->db;
    }

    public function begin()
    {
        return $this->db->beginTransaction();
    }

    public function commit()
    {
        return $this->db->commit();
    }

    public function rollback()
    {
        return $this->db->rollBack();
    }

    public function get_id( $value, $add = false, $int = true )
    {
        if( isset( $this->cache_by_value[$value] ) )
            return $this->cache_by_value[$value];

        if( !isset( $this->query_get_id ) )
        {
            $this->query_get_id = $this->db->prepare( "SELECT id FROM {$this->name} WHERE value = :value" );
            if( $this->query_get_id === false )
            {
                if( $add === false || !self::set_value( $value ) )
                    return false;

                return self::get_id( $value );
            }
        }

        if( $this->query_get_id->execute( [ 'value' => $value ] ) === false )
            return false;

        $id = $this->query_get_id->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $id[0]['id'] ) )
        {
            if( $add === false || !self::set_value( $value ) )
                return false;

            return self::get_id( $value );
        }

        $id = $int ? intval( $id[0]['id'] ) : $id[0]['id'];
        self::set_cache( $id, $value );
        return $id;
    }

    public function get_value( $id, $type )
    {
        if( isset( $this->cache_by_id[$id] ) )
            return $this->cache_by_id[$id];

        if( !isset( $this->query_get_value ) )
        {
            $this->query_get_value = $this->db->prepare( "SELECT value FROM {$this->name} WHERE id = :id" );
            if( $this->query_get_value === false )
                return false;
        }

        if( $this->query_get_value->execute( [ 'id' => $id ] ) === false )
            return false;

        $value = $this->query_get_value->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $value[0]['value'] ) )
        {
            self::set_cache( $id, false );
            return false;
        }

        $value = $value[0]['value'];

        if( $type === 'i' )
            $value = (int)$value;
        else if( $type === 'j' )
            $value = json_decode( $value, true, 512, JSON_BIGINT_AS_STRING );
        else if( $type === 'jz' )
            $value = json_decode( gzinflate( $value ), true, 512, JSON_BIGINT_AS_STRING );

        self::set_cache( $id, $value );
        return $value;
    }

    private function set_value( $value )
    {
        if( !isset( $this->query_set_value ) )
        {
            $this->query_set_value = $this->db->prepare( "INSERT INTO {$this->name}( value ) VALUES( :value )" );
            if( $this->query_set_value === false )
                return false;
        }

        return $this->query_set_value->execute( [ 'value' => $value ] );
    }

    public function set_pair( $id, $value, $type = false )
    {
        self::set_cache( $id, $value );

        if( !isset( $this->query_set_pair ) )
        {
            $this->query_set_pair = $this->db->prepare( "INSERT OR REPLACE INTO {$this->name}( id, value ) VALUES( :id, :value )" );
            if( $this->query_set_pair === false )
                return false;
        }

        if( $type === 'j' )
            $value = json_encode( $value );
        else if( $type === 'jz' )
            $value = gzdeflate( json_encode( $value ), 9 );

        return $this->query_set_pair->execute( [ 'id' => $id, 'value' => $value ] );
    }

    private function set_cache( $id, $value )
    {
        if( count( $this->cache_by_id ) >= $this->cache_size )
            $this->reset_cache();

        $this->cache_by_id[$id] = $value;

        if( isset( $this->cache_by_value ) && !is_array( $value ) && $value !== false )
            $this->cache_by_value[$value] = $id;
    }

    private function reset_cache()
    {
        $this->cache_by_id = [];
        if( isset( $this->cache_by_value ) && count( $this->cache_by_value ) )
            $this->cache_by_value = [];
    }
}
