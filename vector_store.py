import faiss
import numpy as np
from sentence_transformers import SentenceTransformer


model = SentenceTransformer("all-MiniLM-L6-v2")


class VectorStore:
    def __init__(self):
        self.texts = []
        self.index = None

    def build_index(self, texts):
        if not texts:
            raise ValueError("build_index requires at least one text")

        self.texts = list(texts)
        embeddings = model.encode(self.texts, convert_to_numpy=True)
        embeddings = np.asarray(embeddings, dtype="float32")

        dim = embeddings.shape[1]
        self.index = faiss.IndexFlatL2(dim)
        self.index.add(embeddings)

    def search(self, query, k=2):
        if self.index is None:
            raise ValueError("search requires an index built with build_index")

        limit = min(k, len(self.texts))
        query_embedding = model.encode([query], convert_to_numpy=True)
        query_embedding = np.asarray(query_embedding, dtype="float32")

        _, indices = self.index.search(query_embedding, limit)
        return [self.texts[i] for i in indices[0] if i != -1]
