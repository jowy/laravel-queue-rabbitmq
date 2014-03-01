<?php namespace FintechFab\LaravelQueueRabbitMQ\Queue\Jobs;

use Illuminate\Queue\Jobs\Job;
use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Message\AMQPMessage;
use Queue;

class RabbitMQJob extends Job
{

    /**
     * @var string
     */
    protected $queue;

    /**
     * @var AMQPMessage
     */
    protected $message;

    /**
     * @param $container
     * @param AMQPChannel $channel
     * @param $queue
     * @param AMQPMessage $message
     */
    public function __construct($container, AMQPChannel $channel, $queue, AMQPMessage $message)
	{
		$this->container = $container;
		$this->channel = $channel;
		$this->queue = $queue;
		$this->message = $message;

	}

	/**
	 * Fire the job.
	 *
	 * @return void
	 */
	public function fire()
	{
		$this->resolveAndFire(json_decode($this->message->body, true));
	}

	/**
	 * Get the raw body string for the job.
	 *
	 * @return string
	 */
	public function getRawBody()
	{
		return $this->message->body;
	}

	/**
	 * Delete the job from the queue.
	 *
	 * @return void
	 */
	public function delete()
	{
		parent::delete();
		$this->channel->basic_ack($this->message->delivery_info['delivery_tag']);
	}

	/**
	 * Get queue name
	 *
	 * @return string
	 */
	public function getQueue()
	{
		return $this->queue;
	}

    /**
     * @param int $delay
     * @param bool $failure
     */
    public function release($delay = 0, $failure = false)
	{
		$this->delete();

		$body = $this->message->body;
		$body = json_decode($body, true);

        if ($failure)
        {
            $attempts = $this->attempts();

            // write attempts to meta
            $body['data']['attempts'] = $attempts + 1;
        }

		// push back to a queue
		if ($delay > 0) {
			Queue::later($delay, $body['job'], $body['data'], $this->queue);
		} else {
			Queue::push($body['job'], $body['data'], $this->queue);
		}
	}

	/**
	 * Get the number of times the job has been attempted.
	 *
	 * @return int
	 */
	public function attempts()
	{
		$body = json_decode($this->message->body, true);

		return isset($body['data']['attempts']) ? $body['data']['attempts'] : 0;
	}

	/**
	 * Get the job identifier.
	 *
	 * @return string
	 */
	public function getJobId()
	{
		return $this->message->delivery_info['message_id'];
	}

}
