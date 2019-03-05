import boto3


def get_databases(region=None):
    glue = boto3.client('glue', region_name=region)
    result = glue.get_databases()
    databases = []
    for item in result["DatabaseList"]:
        databases.append(item["Name"])
    return databases


def print_databases(region=None):
    print_list(get_databases(region))


def print_list(_list):
    _list.sort()
    for item in _list:
        print(item)

