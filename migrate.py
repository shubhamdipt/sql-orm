from db_models import models
import inspect
import sys


def get_classes_in_module(module):
    return [i[1] for i in inspect.getmembers(models) if inspect.isclass(i[1]) and i[1].__module__ == module]


def main():
    dry_run = False
    if len(sys.argv) > 1:
        if sys.argv[1] == "dry":
            dry_run = True
        else:
            raise ValueError("Invalid arguments. Valid arguments possible: dry")
    for module in get_classes_in_module("db_models.models"):
        module.migrate(dry_run=dry_run)


if __name__ == '__main__':
    main()