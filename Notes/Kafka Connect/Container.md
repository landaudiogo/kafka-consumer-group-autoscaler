# Important File Locations

Connect Properties:
/etc/kafka-connect/kafka-connect.properties

plugin.path: 
/usr/share/java, /usr/share/confluent-hub-components


# Dockerfile

## Dependencies

Download an0roc transformer which allows to transform a whole record to a string:
```

```
> source: https://github.com/an0r0c/kafka-connect-transform-tojsonstring

Install the bigquery sink connector: 
```
confluent-hub install --no-prompt wepay/kafka-connect-bigquery:2.1.3
```
> source: https://www.confluent.io/hub/wepay/kafka-connect-bigquery

## Running 