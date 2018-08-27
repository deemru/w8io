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
        if( $data === false || strlen( $data ) !== 26 )
            return false;

        if( $data[0] !== chr( 1 ) || $data[1] !== W8IO_NETWORK )
            return false;

        $crc = self::sechash( substr( $data, 0, 22 ) );
        if( substr( $crc, 0, 4 ) !== substr( $data, 22, 4 ) )
            return false;

        return true;
    }

    public function get_priv_from_seed( $seed )
    {
        return hash( 'sha256', self::sechash( chr( 0 ) . chr( 0 ) . chr( 0 ) . chr( 0 ) . $seed ), true );
    }

    public function get_pub_from_seed( $seed )
    {
        return sodium_crypto_box_publickey_from_secretkey( self::get_priv_from_seed( $seed ) );
    }

    public function get_address_from_seed( $seed )
    {
        $seed = self::sechash( self::get_pub_from_seed( $seed ) );
        $seed = chr( 1 ) . W8IO_NETWORK . substr( $seed, 0, 20 );
        $seed .= substr( self::sechash( $seed ), 0, 4 );
        return self::b58_encode( $seed );
    }

    public function get_address_from_pubkey( $pubkey )
    {
        $pubkey = self::b58_decode( $pubkey );
        if( $pubkey === false || strlen( $pubkey ) !== 32 )
            return false;
        $pubkey = self::sechash( $pubkey );
        $pubkey = chr( 1 ) . W8IO_NETWORK . substr( $pubkey, 0, 20 );
        $pubkey .= substr( self::sechash( $pubkey ), 0, 4 );
        return self::b58_encode( $pubkey );
    }

    public function generate_address_look( $seed, $look, $limit = 10000 )
    {
        $looklen = strlen( $look );

        for( $i = 0; $i < $looklen; $i++ )
            if( false === strpos( '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz', $look[$i] ) )
                return false;

        $seed = "{$seed}_" . mt_rand( 100000000, 999999999 ) . mt_rand( 100000000, 999999999 ) . mt_rand( 100000000, 999999999 );
        $len = strlen( $seed ) - 1;

        for( $i = 0; $i < $limit; $i++ )
        {
            $address = self::get_address_from_seed( $seed );

            for( $l = 0; $l < $looklen; $l++ )
                if( $address[ 3 + $l ] !== $look[$l] )
                    break;

            if( $l === $looklen )
                return [ $seed, $address ];

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

    public function sign( $data, $key )
    {
        require_once './third_party/curve25519-php/curve25519.php';
        return curve25519\curve25519_sign( $data, $key );
    }

    public function verify( $sign, $data, $key )
    {
        require_once './third_party/curve25519-php/curve25519.php';
        return curve25519\curve25519_verify( $sign, $data, $key );
    }
}
