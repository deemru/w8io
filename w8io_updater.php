<?php

require_once 'w8io_config.php';
require_once './include/w8io_nodes.php';
require_once './include/w8io_blockchain.php';
require_once './include/w8io_blockchain_transactions.php';
require_once './include/w8io_blockchain_balances.php';

function w8io_lock()
{
    require_once './third_party/secqru/include/secqru_flock.php';
    $lock = new secqru_flock( W8IO_DB_DIR . 'w8io.lock' );
    if( false === $lock->open() )
        return false;
    return $lock;
}

function w8io_sleeping_dots( $ticks )
{
    for( $i = 0; $i < $ticks; $i++ )
    {
        echo '.';
        if( sleep( 1 ) )
            w8io_error( "signal" );
    }
}

$lock = w8io_lock();
if( $lock === false )
{
    w8io_trace( 'w', 'w8io_lock() failed' );
    w8io_sleeping_dots( 7 );
    exit;
}

w8io_trace( 'i', 'w8io_updater started' );

function update_proc( $blockchain, $transactions, $balances )
{
    $timer = 0;
    w8io_timer( $timer );

    for( ;; )
    {
        $blockchain_from_to = $blockchain->update();
        if( is_array( $blockchain_from_to ) )
        {
            for( ;; )
            {
                $transactions_from_to = $transactions->update( $blockchain_from_to );
                if( !is_array( $transactions_from_to ) )
                    w8io_error( 'unexpected update transactions error' );
                
                if( $blockchain_from_to['to'] <= $transactions_from_to['to'] )
                    break;

                $blockchain_from_to['from'] = $transactions_from_to['to'];
            }

            for( ;; )
            {
                $balances_from_to = $balances->update( $transactions_from_to );
                if( !is_array( $balances_from_to ) )
                    w8io_error( 'unexpected update balances error' );

                if( $transactions_from_to['to'] <= $balances_from_to['to'] )
                    break;

                $transactions_from_to['from'] = $balances_from_to['to'];
            }
        }
      
        if( $blockchain_from_to['height'] <= $blockchain_from_to['to'] )
            break;
    }

    if( isset( $balances_from_to ) )
        w8io_trace( 's', "{$balances_from_to['from']} >> {$balances_from_to['to']} (" . w8io_ms( w8io_timer( $timer ) ) . ' ms)' );
}

$blockchain = new w8io_blockchain();
$transactions = new w8io_blockchain_transactions();
$balances = new w8io_blockchain_balances();

for( ;; )
{
    update_proc( $blockchain, $transactions, $balances );

    if( sleep( 17 ) )
        w8io_error( "signal" );
}
