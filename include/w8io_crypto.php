<?php

class w8io_crypto
{
    private $b58 = false;
    private $k256 = false;

    public function b58_encode( $data )
    {
        return self::get_b58()->encode( $data );
    }

    public function b58_decode( $string )
    {
        return self::get_b58()->decode( $string );
    }

    public function is_address_valid( $address )
    {
        $data = self::b58_decode( $address );
        if( $data === false || strlen( $data ) != 26 )
            return false;

        if( $data[0] !== chr( 1 ) || $data[1] !== chr( 87 ) )
            return false;
 
        $crc = self::sechash( substr( $data, 0, 22 ) );
        if( substr( $crc, 0, 4 ) !== substr( $data, 22, 4 ) )
            return false;

        return true;
    }

    public function get_address_from_seed( $seed )
    {
        $seed = chr( 0 ) . chr( 0 ) . chr( 0 ) . chr( 0 ) . $seed;
        $seed = self::sechash( sodium_crypto_box_publickey_from_secretkey(
                               hash( 'sha256',
                               self::sechash( $seed ), true ) ) );
        $seed = chr( 1 ) . chr( 87 ) . substr( $seed, 0, 20 );
        $seed .= substr( self::sechash( $seed ), 0, 4 );
        return self::b58_encode( $seed );
    }

    public function get_address_from_pubkey( $pubkey )
    {
        $pubkey = self::b58_decode( $pubkey );
        if( $pubkey === false || strlen( $pubkey ) != 32 )
            return false;
        $pubkey = self::sechash( $pubkey );
        $pubkey = chr( 1 ) . chr( 87 ) . substr( $pubkey, 0, 20 );
        $pubkey .= substr( self::sechash( $pubkey ), 0, 4 );
        return self::b58_encode( $pubkey );
    }

    public function generate_address_look( $seed, $look, $limit = 10000 )
    {
        $looklen = strlen( $look );

        // 3P1vtjFEpXswXWfpiPuFKL1Mqt2NYrL5jVH .. 3PRGViMY7F4QQL6xoRKadp8ddWXdVZZfZoc
        for( $i = 0; $i < $looklen; $i++ )
        {
            if( $i == 0 && false === strpos( '123456789ABCDEFGHJKLMNPQR', $look[$i] ) )
                return false;
            else if( $i == 1 && $look[0] == 'R' && false === strpos( '123456789ABCDEFG', $look[$i] ) )
                return false;
            else if( false === strpos( '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz', $look[$i] ) )
                return false;
        }

        $seed = "{$seed}_" . mt_rand( 100000000, 999999999 ) . mt_rand( 100000000, 999999999 ) . mt_rand( 100000000, 999999999 );
        $len = strlen( $seed ) - 1;

        for( $i = 0; $i < $limit; $i++ )
        {
            $address = self::get_address_from_seed( $seed );

            for( $l = 0; $l < $looklen; $l++ )
                if( $address[ 2 + $l ] !== $look[$l] )
                    break;

            if( $l == $looklen )
                return array( $seed, $address );

            for( $n = $len;; $n-- )
            {
                if( $seed[$n] !== '0' )
                {
                    $seed[$n] = $seed[$n] - 1;
                    break;
                }

                $seed[$n] = '9';
            }
        }

        return false;
    }

    private function get_b58()
    {
        if( $this->b58 === false )
        {
            require_once './third_party/secqru/include/secqru_abcode.php';
            $this->b58 = new secqru_abcode( '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz' );
        }

        return $this->b58;
    }

    private function get_k256()
    {
        if( $this->k256 === false )
        {
            require_once './third_party/php-keccak/src/Keccak.php';
            $this->k256 = new kornrunner\Keccak();
        }

        return $this->k256;
    }

    public function keccak256( $data )
    {
        return self::get_k256()->hash( $data, 256, true );
    }

    public function blake2b256( $data )
    {
        return sodium_crypto_generichash( $data );
    }

    public function sechash( $data )
    {
        return self::keccak256( self::blake2b256( $data ) );
    }
}
