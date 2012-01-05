"""
Routes messages to different processors.

Rules are of the form:
    condition => target
Where each on of condition and target are of the form:
    name [arguments]*
And the arguments are dependant on the name.

Each rule is applied to the message on the order they are specified.

Conditions supported:
    any: will match all messages

Targets supported:
    drop: will drop the message, stopping any further processing.
"""

from txstatsd.server.processor import BaseMessageProcessor


class StopProcessingException(Exception):

    pass


class Router(BaseMessageProcessor):

    def __init__(self, message_processor, rules_config):
        """Configure a router with rules_config.

        rules_config is a new_line separeted list of rules.
        """
        self.rules_config = rules_config
        self.message_processor = message_processor
        self.rules = self.build_rules(rules_config)

    def build_rules(self, rules_config):
        rules = []
        for line in rules_config.split("\n"):
            if not line:
                continue

            condition, target = line.split("=>")

            condition_parts = [
                p.strip() for p in condition.split(" ") if p]
            condition_factory = getattr(self,
                "build_condition_" + condition_parts[0], None)
            if condition_factory is None:
                raise ValueError("unknown condition %s" %
                                (condition_parts[0],))

            target_parts = [
                p.strip() for p in target.split(" ") if p]

            target_factory = getattr(self,
                "build_target_" + target_parts[0], None)

            if target_factory is None:
                raise ValueError("unknown target %s" %
                                (target_parts[0],))

            rules.append((
                condition_factory(*condition_parts[1:]),
                target_factory(*target_parts[1:])))
        return rules

    def build_condition_any(self):
        return lambda *args: True

    def build_target_drop(self):
        def drop(*args):
            raise StopProcessingException()
        return drop

    def process_message(self, message, metric_type, key, fields):
        try:
            for condition, target in self.rules:
                if condition(message, metric_type, key, fields):
                    target(message, metric_type, key, fields)

            self.message_processor.process_message(message, metric_type,
                                                     key, fields)
        except StopProcessingException:
            pass
