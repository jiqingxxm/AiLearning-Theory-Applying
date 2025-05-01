import re

def clean_text(text):
    """
    Remove any parenthetical segments (including Chinese parentheses) and trim whitespace.
    For example, "This is a sentence(remark)" -> "This is a sentence"
    
    Parameters:
        text (str): The text to clean.
        
    Returns:
        str: The cleaned text.
    """
    pattern = r'[\(（][^\)）]*[\)）]'  # Pattern to match parentheses and their contents
    cleaned_text = re.sub(pattern, '', text).strip()
    return cleaned_text

def find_dict_by_element(dict_list, target_element):
    """
    Given a list of dictionaries, return all dictionaries where  '列名中文描述' contains the target_element.
    Parameters:
        dict_list (list): A list of dictionaries, each expected to have '列名中文描述' key.
        target_element (str): The element to search for.
        
    Returns:
        list: A list of dictionaries that contain target_element in '列名中文描述'.
    """
    return [d for d in dict_list if target_element in d.get('列名中文描述', [])]