import json
import requests

# the json_parser functions are implemented in order to make the code more dynamic,
# the function will work for different variables (s.a. different number of random users to be generate ect.)
# 
def get_values(d):
    """
    return the values of a random user
    """
    for v in d.values():
        if isinstance(v, dict):
            yield from get_values(v)
        else:
            yield v
            
def get_keys(d, curr_key=[]):
    """
    return the fields of the random user obj
    """
    for k, v in d.items():
        if isinstance(v, dict):
            yield from get_keys(v , curr_key + [k])
        elif isinstance(v, list):
            for i in v:
                yield from get_keys(i, curr_key + [k])
        else:
            yield '.'.join(curr_key + [k])
            
def clean_keys(response):
    """
    return a better format for the keys. the NVARCHAR datatype used to overcome the fact
    that we don't know the fields of the random user and even if we did, they might change,
    so this arbitrary decision keeps the code dynamic 
    """
    keys = ([*get_keys(response.json()["results"][0])])
    return (' NVARCHAR(100), '.join("`" + x + "`" for x in keys) + " NVARCHAR(100)")

def clean_values(response):
    """
    return a random user values in a better format in order to insert them to the database. 
    """
    values = check(list(get_values(response)))
    return (', '.join("'" + str(x) + "'" for x in values))
            

def check(values):
    """
    check for invalid charachter inside random_user values
    and return the valid version
    """
    for index,val in enumerate(values):
        check = str(val)
        if '\'' in check:
            check = check.replace('\'', '\'\'')
            values[index] = check
    return values


