<?php namespace FintechFab\LaravelQueueRabbitMQ\Queue\Connectors;

//use AMQPConnection;

use PhpAmqpLib\Connection\AMQPConnection;

use FintechFab\LaravelQueueRabbitMQ\Queue\RabbitMQQueue;
use Illuminate\Queue\Connectors\ConnectorInterface;

class RabbitMQConnector implements ConnectorInterface
{

	/**
	 * Establish a queue connection.
	 *
	 * @param  array $config
	 *
	 * @return \Illuminate\Queue\QueueInterface
	 */
	public function connect(array $config)
	{

		// create connection with AMQP
		$connection = new AMQPConnection(
            isset($config['host'])                  ? $config['host']                   : '127.0.0.1',
            isset($config['port'])                  ? $config['port']                   : 5672,
            isset($config['login'])                 ? $config['login']                  : 'guest',
            isset($config['password'])              ? $config['password']               : 'guest',
            isset($config['vhost'])                 ? $config['vhost']                  : '/',
            isset($config['insist'])                ? $config['insist']                 : false,
            isset($config['login_method'])          ? $config['login_method']           : 'AMQPLAIN',
            isset($config['login_response'])        ? $config['login_response']         : null,
            isset($config['locale'])                ? $config['locale']                 : 'en_US',
            isset($config['connection_timeout'])    ? $config['connection_timeout']     : 3,
            isset($config['read_write_timeout'])    ? $config['read_write_timeout']     : 3,
            isset($config['context'])               ? $config['context']                : null
        );

		if (!isset($config['exchange_type'])) {
			$config['exchange_type'] = 'direct';
		}

		if (!isset($config['exchange_flags'])) {
			$config['exchange_flags'] = array('durable');
		}

		return new RabbitMQQueue(
			$connection,
			$config['exchange_name'],
			$config['exchange_type'],
			$config['exchange_flags']
		);
	}
}