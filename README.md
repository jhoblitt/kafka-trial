# kafka-trial

## Example Usage

```bash
export KT_TOPIC="example.topic"
export KT_BOOTSTRAP_SERVERS="example.com:9094"
export KT_SASL_USERNAME="user"
export KT_SASL_PASSWORD="XXX"

./kafka-trial --messages 10000 --writers 200
```

## Example Output

```bash
 ~ $ ./kafka-trial --messages 10000 --writers 1000
2025/03/06 02:07:45 Created topic "lsst.sal.notactuallysal.test"
2025/03/06 02:08:26 Listening for messages on topic "lsst.sal.notactuallysal.test"
2025/03/06 02:18:33 Deleting topic "lsst.sal.notactuallysal.test"
2025/03/06 02:18:33 Total messages: 10000000
2025/03/06 02:18:33 Producers took 10m48.655818885s
2025/03/06 02:18:33 Producers message rate 15416.50/s
2025/03/06 02:18:33 Consumers took 10m48.655819637s
2025/03/06 02:18:33 Consumers message rate 15416.50/s
2025/03/06 02:18:33 Consumers mean latency: 99.859373ms
```
