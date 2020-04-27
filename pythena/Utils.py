import boto3


def get_databases(region=None, session=None):
    if session is None:
        session = boto3.session.Session()
    glue = session.client('glue', region_name=region)
    databases = []

    params = {}

    while True:
        result = glue.get_databases(**params)

        for item in result["DatabaseList"]:
            databases.append(item["Name"])
        if result.get('NextToken') is None:
            break
        else:
            params['NextToken'] = result.get('NextToken')

    return databases


def print_databases(region=None, session=None):
    print_list(get_databases(region=region, session=session))


def print_list(_list):
    _list.sort()
    for item in _list:
        print(item)

