# kafka-trial

## Example Usage

```bash
export KT_TOPIC="example.topic"
export KT_BOOTSTRAP_SERVERS="example.com:9094"
export KT_SASL_USERNAME="user"
export KT_SASL_PASSWORD="XXX"

./kafka-trial --messages 10000 --writers 200
```
