<?php

class secqru_flock
{
    private $fp;
    private $filename;
    private $delay;
    private $timeout;

    public function __construct( $filename,
                                 $delay = 100000 /* 0.1 sec */,
                                 $timeout = 1000000 /* 1 sec */ )
    {
        $this->fp = false;
        $this->filename = $filename;
        $this->delay = $delay;
        $this->timeout = $timeout;
    }

    public function __destruct()
    {
        self::close();
    }

    public function open( $access = 'a+' )
    {
        $this->fp = fopen( $this->filename, $access );

        if( $this->fp === false )
            return false;

        $timer = 0;
        $mode = $access[0] == 'r' ? LOCK_SH : LOCK_EX;
        $blocking = $this->delay ? LOCK_NB : 0;

        for( ;; )
        {
            if( flock( $this->fp, $mode | $blocking ) )
                return true;

            $timer += $this->delay;
            if( $timer > $this->timeout )
                break;

            usleep( $this->delay );
        }

        fclose( $this->fp );
        $this->fp = false;
        return false;
    }

    public function close()
    {
        if( $this->fp )
        {
            flock( $this->fp, LOCK_UN );
            fclose( $this->fp );
            $this->fp = false;
        }
    }

    public function append( $data )
    {
        if( self::open( 'a+' ) === false )
            return false;

        fwrite( $this->fp, $data );
        self::close();
        return true;
    }

    public function get()
    {
        if( self::open( 'r' ) === false )
            return false;

        $data = fread( $this->fp, filesize( $this->filename ) );
        self::close();
        return $data;
    }

    public function put( $data )
    {
        if( self::open( 'w+' ) === false )
            return false;

        fwrite( $this->fp, $data );
        self::close();
        return $data;
    }
}
