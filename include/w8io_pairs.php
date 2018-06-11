<?php

class w8io_pairs
{
    private $db;
    private $name;
    private $cache_size;
    private $cache_by_id;
    private $cache_by_value;

    private $query_get_id = false;
    private $query_max_id = false;
    private $query_get_value = false;
    private $query_set_value = false;
    private $query_set_pair = false;

    public function __construct( $db, $name, $writable = false, $type = 'INTEGER PRIMARY KEY|TEXT UNIQUE|0|0', $cache_size = 2048 )
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
        }
        
        $this->name = $name;
        $this->cache_size = $cache_size;
        $this->cache_by_id = array();
        $this->cache_by_value = array();

        if( $writable )
        {
            $this->db->exec( W8IO_DB_PRAGMAS );

            $type = explode( '|', $type );

            $this->db->exec( "CREATE TABLE IF NOT EXISTS {$this->name}( id {$type[0]}, value {$type[1]} )" );

            if( $type[2] )
                $this->db->exec( "CREATE INDEX IF NOT EXISTS {$this->name}_id_index ON {$this->name}( id )" );

            if( $type[3] )
                $this->db->exec( "CREATE INDEX IF NOT EXISTS {$this->name}_value_index ON {$this->name}( value )" );
        }
    }

    public function reset()
    {
        $this->db->exec( "DELETE FROM {$this->name}" );
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

        if( $this->query_get_id === false )
        {
            $this->query_get_id = $this->db->prepare( "SELECT id FROM {$this->name} WHERE value = :value" );
            if( $this->query_get_id === false )
            {
                if( $add === false || !self::set_value( $value ) )
                    return false;

                return self::get_id( $value );
            }
        }

        if( $this->query_get_id->execute( array( 'value' => $value ) ) === false )
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

    public function max_id()
    {
        if( $this->query_max_id === false )
        {
            $this->query_max_id = $this->db->prepare( "SELECT id FROM {$this->name} ORDER BY id DESC LIMIT 1" );
            if( $this->query_max_id === false )
                return false;
        }

        if( $this->query_max_id->execute() === false )
            return false;

        $id = $this->query_max_id->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $id[0]['id'] ) )
            return false;

        return intval( $id[0]['id'] );
    }

    public function get_value( $id )
    {
        if( isset( $this->cache_by_id[$id] ) )
            return $this->cache_by_id[$id];

        if( $this->query_get_value === false )
        {
            $this->query_get_value = $this->db->prepare( "SELECT value FROM {$this->name} WHERE id = :id" );
            if( $this->query_get_value === false )
                return false;
        }

        if( $this->query_get_value->execute( array( 'id' => $id ) ) === false )
            return false;

        $value = $this->query_get_value->fetchAll( PDO::FETCH_ASSOC );

        if( !isset( $value[0]['value'] ) )
            return false;

        $value = $value[0]['value'];
        self::set_cache( $id, $value );
        return $value;
    }

    public function set_value( $value )
    {
        if( $this->query_set_value === false )
        {
            $this->query_set_value = $this->db->prepare( "INSERT INTO {$this->name}( value ) VALUES( :value )" );
            if( $this->query_set_value === false )
                return false;
        }

        if( $this->query_set_value->execute( array( 'value' => $value, ) ) === false )
            return false;

        return true;
    }

    public function set_pair( $id, $value )
    {
        if( $this->query_set_pair === false )
        {
            $this->query_set_pair = $this->db->prepare( "INSERT OR REPLACE INTO {$this->name}( id, value ) VALUES( :id, :value )" );
            if( $this->query_set_pair === false )
                return false;
        }

        if( $this->query_set_pair->execute( array( 'id' => $id, 'value' => $value, ) ) === false )
            return false;

        self::set_cache( $id, $value );
        return true;
    }

    private function set_cache( $id, $value )
    {
        if( count( $this->cache_by_id ) >= $this->cache_size )
        {
            $this->cache_by_id = array();
            $this->cache_by_value = array();
        }

        $this->cache_by_id[$id] = $value;
        $this->cache_by_value[$value] = $id;
    }
}
