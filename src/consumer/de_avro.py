import json

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



class DummyItem: 


    def __init__(self, d): 
        self.d = d

    def _asdict(self):
        print(self.d)
        return self.d 


class DETestV1Topic: 


    def deserialize(self, stream, item_type_name): 
        received_dict = json.loads(stream.getvalue())
        return DummyItem(received_dict)
