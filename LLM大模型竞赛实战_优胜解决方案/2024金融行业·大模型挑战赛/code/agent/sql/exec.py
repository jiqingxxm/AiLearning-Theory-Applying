# agent/sql/exec.py
from config import *
import requests

def exec_sql_s(sql):
    """
    Execute a given SQL query on a remote endpoint and return the result.
    Uses 'Access_Token' for authorization and limits the result to 10 rows.

    Parameters:
        sql (str): The SQL query to be executed.

    Returns:
        list: The query result as a list of rows (dictionaries), or None if not found.
    """
    headers = {
        "Authorization": f'Bearer {Access_Token}',
        "Accept": "application/json"
    }
    url = "https://comm.chatglm.cn/finglm2/api/query"

    response = requests.post(url, headers=headers, json={
        "sql": sql,
        "limit": 50
    })
    response_json = response.json()

    # If there's no 'data' field, print the full response for debugging
    if 'data' not in response_json:
        if DEBUG_VER == 3:
            print(response_json)
        pass

    # Return 'data' if present
    return response_json.get('data', None)