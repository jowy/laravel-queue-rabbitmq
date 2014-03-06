<?php namespace FintechFab\LaravelQueueRabbitMQ\Queue;

use FintechFab\LaravelQueueRabbitMQ\Queue\Jobs\RabbitMQJob;
use Illuminate\Queue\Queue;
use Illuminate\Queue\QueueInterface;

use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitMQQueue extends Queue implements QueueInterface
{

    /**
     * @var AMQPConnection
     */
    protected $connection;

    /**
     * @var AMQPChannel
     */
    protected $channel;

    /**
     * @var string
     */
    protected $exchange;

    /**
     * @param AMQPConnection $amqpConnection
     * @param $exchange
     * @param $exchange_type
     * @param $exchange_flags
     */
    public function __construct(AMQPConnection $amqpConnection, $exchange, $exchange_type, $exchange_flags)
    {
        $this->connection = $amqpConnection;
        $this->exchange = $exchange;
        $this->channel = $this->connection->channel();
        $this->channel->exchange_declare(
            $exchange,
            $exchange_type,
            in_array('passive', $exchange_flags),
            in_array('durable', $exchange_flags),
            in_array('auto_delete', $exchange_flags),
            in_array('internal', $exchange_flags),
            in_array('nowait', $exchange_flags)
        );
    }

    /**
     * Push a new job onto the queue.
     *
     * @param  string $job
     * @param  mixed  $data
     * @param  string $queue
     *
     * @return bool
     */
    public function push($job, $data = '', $queue = null)
    {
        $payload = $this->createPayload($job, $data);

        return $this->pushRaw($payload, $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string $queue
     * @param  array  $options
     *
     * @throws \AMQPException
     * @return mixed
     */
    public function pushRaw($payload, $queue = null, array $options = array())
    {
        $channel = $this->connection->channel();
        $channel->queue_declare($queue, false, true, false, false);
        $channel->queue_bind($queue, $this->exchange, $queue);

        $message = new AMQPMessage($payload, array_merge($options, array(
            'content_type' => 'application/json',
            'delivery_mode' => 2,
        )));

        $channel->basic_publish($message, $this->exchange, $queue);

        $channel->close();

        return true;
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  \DateTime|int $delay
     * @param  string        $job
     * @param  mixed         $data
     * @param  string        $queue
     *
     * @throws \AMQPException
     * @return mixed
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        $payload = $this->createPayload($job, $data);

        $channel = $this->connection->channel();

        $channel->queue_declare($queue, true, true, false, false, false, array(
            'x-dead-letter-exchange'    => $this->exchange,
            'x-dead-letter-routing-key' => $queue,
            'x-message-ttl'             => $delay * 1000,
        ));

        $channel->queue_bind($queue, $this->exchange);

        $message = new AMQPMessage($payload, array(
            'content_type' => 'application/json',
            'delivery_mode' => 2,
        ));

        $channel->basic_publish($message, $this->exchange);
        $channel->close();
        return true;

    }

    /**
     * Pop the next job off of the queue.
     *
     * @param string|null $queue
     *
     * @return \Illuminate\Queue\Jobs\Job|null
     */
    public function pop($queue = null)
    {
        $channel = $this->connection->channel();
        $channel->queue_declare($queue, true, true, false, false);
        $channel->queue_bind($queue, $this->exchange);

        $message = $channel->basic_get($queue);

        if ($message instanceof AMQPMessage) {
            return new RabbitMQJob($this->container, $channel, $queue, $message);
        }

        $channel->close();

        return null;
    }

}