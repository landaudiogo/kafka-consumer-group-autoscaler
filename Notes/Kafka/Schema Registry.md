Links: 
> Overview:
> https://docs.confluent.io/platform/current/schema-registry/index.html#schemaregistry-intro
> 
> Managing schemas for topics:
> https://docs.confluent.io/platform/current/control-center/topics/schema.html


A schema is a way of defining how the data has to be inserted into a topics. It defines a set of rules the record's format and content, have to comply with.

This sets a format that all producers have to use when publishing messages to a given topic (regardless of the partition)

Usually the formats are set on the record's value, indicating what fields and types the fields have to be in.

