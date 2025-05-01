# utils/json_utils.py
from config import *
from agent.prompt import create_chat_completion
import re
import json

def find_json(text):
    """
    Attempt to extract and parse a JSON object from the provided text.
    The function tries up to three attempts using two patterns:
      1. A Markdown code block with ```json ... ```
      2. A more general JSON-like pattern using { and }

    If successful, returns the parsed JSON data.
    If parsing fails after all attempts, returns the original text.
    
    Parameters:
        text (str): The input text from which to extract JSON.
    
    Returns:
        dict or str: Parsed JSON dictionary if successful, else the original text.
    """
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        json_pattern = r"```json\n(.*?)\n```"
        match = re.search(json_pattern, text, re.DOTALL)
        if not match:
            json_pattern2 = r"({.*?})"
            match = re.search(json_pattern2, text, re.DOTALL)

        if match:
            json_string = match.group(1) if match.lastindex == 1 else match.group(0)
            # Remove Markdown formatting if present
            json_string = json_string.replace("```json\n", "").replace("\n```", "")
            try:
                data = json.loads(json_string)
                return data
            except json.JSONDecodeError as e:
                if DEBUG_VER == 3:
                    print(f"--------------------find_json if match error text:{text}")
                if attempt < max_attempts:
                    text = fix_json(json_string, e, model=MODEL)
                    if DEBUG_VER == 3:
                        print(f"Attempt {attempt}: Failed to parse JSON, reason: {e}. Retrying...")
                else:
                    if DEBUG_VER == 3:
                        print(f"All {max_attempts} attempts to parse JSON failed. Returning original text.")
                    pass
        else:
            if DEBUG_VER == 3:
                print(f"--------------------find_json else text:{text}")
            if attempt < max_attempts:
                if DEBUG_VER == 3:
                    print(f"Attempt {attempt}: No JSON string found in the text. Retrying...")
                pass
            else:
                if DEBUG_VER == 3:
                    print("No matching JSON string found. Returning original text.")
                pass

        # If no match or no success in this attempt, return the original text
        return text


def fix_json(text, json_error, model):
    """
    修复JSON字符串，使其成为有效的JSON。
    """
    NAIVE_FIX = f"""Instructions:
    --------------
    请修复JSON字符串，使其成为有效的JSON。
    --------------

    下面是原始的JSON字符串：
    --------------
    {text}
    --------------
    下面是的错误信息：
    --------------
    {json_error}
    --------------

    请仅回复json，用```json ... ```包裹json字符串："""
    
    messages = [{"role": "user", "content": NAIVE_FIX}]
    response = create_chat_completion(messages, model)
    answer = response.choices[0].message.content
    return answer

def dict_to_sentence(data):
    """
    Convert a dictionary into a descriptive sentence by enumerating key-value pairs.
    For example: {"name": "John", "age": 30} -> "name 是 John, age 是 30"
    
    Parameters:
        data (dict): The dictionary to convert.
        
    Returns:
        str: A sentence describing the dictionary keys and values.
    """
    try:
        if not isinstance(data, dict):
            raise ValueError("Input is not a dictionary")

        return ", ".join(f"{key} 是 {value}" for key, value in data.items())
    except Exception as e:
        if DEBUG_VER == 3:
            print(f"Error in dict_to_sentence: {e}")
        return str(data)