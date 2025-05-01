# agent/embeddings/embedding_utils.py
import json
import numpy as np
from config import *
from zhipuai import ZhipuAI


def save_embeddings(text_list, save_path, batch_size=64):
    """
    保存文本的向量到本地文件，分批请求最多 64 个文本。
    """
    all_embeddings = []
    
    # 按批次处理文本
    for i in range(0, len(text_list), batch_size):
        chunk = text_list[i:i+batch_size]
        
        # 批量请求文本的嵌入向量
        response = client.embeddings.create(
            model="embedding-3",
            input=chunk
        )
        
        # 提取每个文本的 embedding
        embeddings = [item.embedding for item in response.data]
        all_embeddings.extend(embeddings)  # 累加所有的 embeddings
    
    # 保存到文件
    with open(save_path, 'w', encoding='utf-8') as f:
        json.dump(all_embeddings, f)
    
    print(f"Embeddings saved to {save_path}")

def calculate_similarity(text_list, local_vectors=False):
    """
    批量计算相似度：
      - 输入一个文本列表，其中第一个文本作为基准，其余文本与基准比较
      - 当候选文本超过 64 条时，会分批请求，最后返回所有候选文本与基准文本的相似度（保留4位小数）
    """

    base_text = text_list[0]
    # 先单独请求基准文本的 embedding
    base_response = client.embeddings.create(
        model="embedding-3",
        input=[base_text]
    )
    base_embedding = base_response.data[0].embedding
    base_embedding = np.array(base_embedding)

    all_similarities = []
    if local_vectors:
        if DEBUG_VER == 3:
            print(f'------>local_vectors:{local_vectors}')
        # 使用本地保存的向量
        with open(local_vectors, 'r', encoding='utf-8') as f:
            local_embeddings = json.load(f)
        
        # 确保读取到的向量与候选文本对应
        candidate_embeddings = np.array(local_embeddings)
    else:
        # 批量处理候选文本，每次最多请求64条
        candidate_texts = text_list[1:]
        candidate_embeddings = []
        chunk_size = 64  # 每次最多请求64条
        for i in range(0, len(candidate_texts), chunk_size):
            chunk = candidate_texts[i:i+chunk_size]
            response = client.embeddings.create(
                model="embedding-3",
                input=chunk
            )
            # 提取候选文本的 embedding 并转换为 NumPy 数组
            embeddings = [item.embedding for item in response.data]
            candidate_embeddings.extend(embeddings)
        
        candidate_embeddings = np.array(candidate_embeddings)

    # 计算余弦相似度： dot / (||base|| * ||candidate||)
    dot_products = candidate_embeddings.dot(base_embedding)
    norm_base = np.linalg.norm(base_embedding)
    norm_candidates = np.linalg.norm(candidate_embeddings, axis=1)
    similarities = dot_products / (norm_base * norm_candidates)

    # 保留4位小数，并加入结果列表
    sims = [round(float(sim), 4) for sim in similarities]
    all_similarities.extend(sims)

    return all_similarities
