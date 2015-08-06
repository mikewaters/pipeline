from pipeline.actions import action

@action
def echo_test_command(self, source, value):
    return value

