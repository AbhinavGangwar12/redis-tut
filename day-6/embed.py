import numpy as np
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2") # dim - 384

def embed(text):
    return model.encode(text, normalize_embeddings=True).astype(np.float32)

if __name__ == "__main__":
    a = embed("How do I reset my password?")
    b = embed("I forgot my login credentials")
    c = embed("Best pizza toppings in Italy")
    print(a.shape, a.dtype)
    print(round(float(a @ b), 2), round(float(a @ c), 2))