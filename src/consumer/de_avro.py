TopicPartitionsSchema = {
    "name": "TopicPartition",
    "type": "record",
    "fields": [
        {"name": "topic_name", "type": "string"},
        {"name": "bq_table", "type": "string"},
        {
            "name": "topic_class", 
            "type": {
                "name": "topic_class",
                "type": "record",
                "fields": [
                    {"name": "module_path", "type": "string"},
                    {"name": "class_name", "type": "string"},
                ]
            }
        },
        {
            "name": "partitions", 
            "type": {
                "type": "array", 
                "items": {
                    "name": "partition",
                    "type": "int"
                }
            }
        },
        {
            "name": "ignore_events", 
            "type": {
                "type": "array", 
                "items": {
                    "name": "event_type",
                    "type": "string"
                }
            }
        },
    ]
}

DEControllerSchema = {
    "name": "DEControllerSchema",
    "type": "array",
    "items": TopicPartitionsSchema
}
