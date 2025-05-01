# coding=utf-8
from config import *
from agent.brain import main_answer
import time
import json
import sys

if __name__ == "__main__":
    '''
      online devlop 
    '''
    in_param_path = sys.argv[1]
    out_path = sys.argv[2]
    
    try:
        main_start_time = time.time()
        with open(in_param_path, 'r') as load_f:
            input_params = json.load(load_f)
        questionFile = input_params["fileData"]["questionFilePath"]
        # 读取问题文件
        with open(questionFile, 'r') as load_f:
            q_json_list = json.load(load_f)

        idx_ranges = [(0,100)]
        results = main_answer(q_json_list, out_path, idx_ranges)
        main_elapsed_time = time.time() - main_start_time
        print(f"Completed main in {main_elapsed_time:.2f} seconds")
    except Exception as e:
        print("Error: %s" % e)
        sys.exit(1) 