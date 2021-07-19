import logging

from event_stream.event_stream_consumer import EventStreamConsumer


class UpdateConsumer(EventStreamConsumer):
    state = "linked"
    relation_type = "discusses"

    log = "UpdateConsumer "
    group_id = "update_consumer"

    def __init__(self, id, manager):
        super().__init__(id)
        self.manager = manager

    def on_message(self, json_msg):
        logging.warning(self.log + "on message UpdateConsumer")

        self.manager.broadcast(f"Tweet: " + json_msg)
