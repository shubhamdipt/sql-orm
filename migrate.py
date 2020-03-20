from db_models import models
import inspect
import sys


def get_classes_in_module(module):
    model_classes = []
    for name, obj in inspect.getmembers(models):
        if inspect.isclass(obj) and obj.__module__ == module:
            source, start_line = inspect.getsourcelines(obj)
            model_classes.append([name, obj, start_line])
    return [i[1] for i in sorted(model_classes, key=lambda x: x[2])]


def run_migrations():
    dry_run = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "dry":
            dry_run = True
        else:
            raise ValueError("Invalid arguments. Valid arguments possible: dry")
    for module in get_classes_in_module("db_models.models"):
        module.migrate(dry_run=dry_run)


if __name__ == '__main__':
    run_migrations()