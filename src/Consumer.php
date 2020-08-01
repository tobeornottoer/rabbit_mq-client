<?php
/**
 * Class Consumer
 * Created by PhpStorm.
 * Author: jw
 * Time:23:07
 * @package Jm
 * rabbitMq 消费者
 */

namespace Jm;


class Consumer
{
    public $connect = null;
    private $channel = null;
    private $exchange = null;
    public $queue = null;
    private $route = null;

    /**
     * Producer constructor.
     * @param null $config = [
     *      "host" => "127.0.0.1",
     *      "port" => 5672,
     *      "vhost" => "/",
     *      "login" => "guest",
     *      "password" => "guest",
     *      "exchange_name" => "exchange_1",
     *      "exchange_type" => AMQP_EX_TYPE_DIRECT:直连交换机 / AMQP_EX_TYPE_FANOUT:扇形交换机 / AMQP_EX_TYPE_HEADERS:头交换机 / AMQP_EX_TYPE_TOPIC:主题交换机
     *      "exchange_flags" => AMQP_DURABLE - 交换机持久化
     *      "queue_name" => "exchange_1",
     *      "queue_flags" => AMQP_DURABLE - 消息队列持久化
     *      "route" => "sms" - 消息的路由
     * ]
     */
    public function __construct($config=null)
    {
        if(!empty($config) && is_array($config)){
            $this->configInit($config);
        }else{
            $this->connect = new \AMQPConnection();
        }
    }

    public function configInit(array $config){
        $connect_conf = array(
            "host" => $config["host"],
            "port" => $config["port"],
            "vhost" => $config["vhost"]?:"/",
            "login" => $config["login"],
            "password" => $config["password"]
        );
        $this->connect = new \AMQPConnection($connect_conf);
        $this->connect();
        $this->setExchangeName($config["exchange_name"]);
        $this->setExchangeType($config["exchange_type"]);
        $this->setExchangeFlags($config["exchange_flags"]);
        $this->setQueueName($config["queue_name"]);
        $this->setQueueFlags($config["queue_flags"]);
        if(isset($config["route"]) && !empty($config["route"])){
            $this->setRoute($config["route"]);
        }
    }

    public function setHost(string $host){
        $this->connect->setHost($host);
        return $this;
    }

    public function setPort(int $port){
        $this->connect->setPort($port);
        return $this;
    }

    public function setVhost(string $vhost){
        $this->connect->setVhost($vhost);
        return $this;
    }

    public function setLogin(string $login){
        $this->connect->setLogin($login);
        return $this;
    }

    public function setPassword(string $password){
        $this->connect->setPassword($password);
        return $this;
    }

    public function connect(){
        if(!$this->connect->connect()){
            throw new \Exception("MQ连接失败");
        }
        $this->channel = new \AMQPChannel($this->connect);
        $this->exchange = new \AMQPExchange($this->channel);
        $this->queue = new \AMQPQueue($this->channel);
        return $this;
    }

    public function setExchangeName(string $name){
        $this->exchange->setName($name);
        return $this;
    }

    public function setExchangeType(string $type){
        $this->exchange->setType($type);
        return $this;
    }

    public function setExchangeFlags(int $flags){
        $this->exchange->setFlags($flags);
        return $this;
    }

    public function setQueueName(string $name){
        $this->queue->setName($name);
        return $this;
    }

    public function setQueueFlags(string $flags){
        $this->queue->setFlags($flags);
        return $this;
    }

    public function setRoute(string $route){
        $this->route = $route;
        return $this;
    }

    public function receive($route=null){
        $this->exchange->declareExchange();
        $this->queue->declareQueue();
        $route = $route ?: $this->route;
        $this->queue->bind($this->exchange->getName(), $route);
        $this->queue->consume([$this,"execute"]);
    }

    /**
     * @param $envelope
     * @param $queue
     * 执行消费
     */
    public function execute($envelope, $queue){
        echo $envelope->getBody()."\n";
        //这里必须发送应答信号，不然MQ消息越来越多
        $queue->ack($envelope->getDeliveryTag());
    }

}