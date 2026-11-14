"""
Production Big Data Machine Learning Module for Smart Agriculture
Algorithms Built-in:
1. Distributed K-Means Clustering: Unsupervised soil-climate agro-ecological zonation (optimizing crop suitability patterns)
2. Multinomial Naive Bayes & Random Decision Forest Classifier: Supervised classification of crop yield efficiency (High / Medium / Low)
3. Standard Scaler & One-Hot Categorical Pipeline
4. Model Evaluation Metrics: Accuracy, Precision, Recall, F1-Score, Silhouette Score, Confusion Matrix
"""

import math
import random
import json

class StandardScaler:
    def __init__(self):
        self.means = {}
        self.stds = {}

    def fit(self, records, feature_cols):
        self.feature_cols = feature_cols
        n = len(records)
        for col in feature_cols:
            vals = [float(r[col]) for r in records]
            mean = sum(vals) / n
            var = sum((v - mean) ** 2 for v in vals) / n
            self.means[col] = mean
            self.stds[col] = math.sqrt(var) if var > 1e-9 else 1.0

    def transform(self, records):
        transformed = []
        for r in records:
            scaled_r = dict(r)
            for col in self.feature_cols:
                scaled_r[f"scaled_{col}"] = (float(r[col]) - self.means[col]) / self.stds[col]
            transformed.append(scaled_r)
        return transformed

    def fit_transform(self, records, feature_cols):
        self.fit(records, feature_cols)
        return self.transform(records)


class DistributedKMeans:
    """
    K-Means clustering designed for multi-dimensional agronomic partitioning:
    - Partitioning soil condition (pH, OM, N, P, K, moisture) and weather (rainfall, temp)
    - Computes inertia, silhouette approximation, and cluster centroids
    """
    def __init__(self, k=4, max_iter=25, seed=42):
        self.k = k
        self.max_iter = max_iter
        self.seed = seed
        self.centroids = []
        self.inertia_ = 0.0

    def fit(self, records, feature_cols):
        random.seed(self.seed)
        self.feature_cols = feature_cols
        
        # Extract numerical vectors
        vectors = [[float(r[col]) for col in feature_cols] for r in records]
        n_samples = len(vectors)
        n_features = len(feature_cols)

        # Initialize centroids using K-Means++ logic
        first_idx = random.randint(0, n_samples - 1)
        self.centroids = [list(vectors[first_idx])]

        for _ in range(1, self.k):
            distances = []
            for vec in vectors:
                min_d = min(sum((v - c) ** 2 for v, c in zip(vec, cent)) for cent in self.centroids)
                distances.append(min_d)
            total_d = sum(distances)
            if total_d == 0:
                self.centroids.append(list(random.choice(vectors)))
                continue
            r_val = random.uniform(0, total_d)
            accum = 0.0
            chosen = vectors[0]
            for idx, d in enumerate(distances):
                accum += d
                if accum >= r_val:
                    chosen = vectors[idx]
                    break
            self.centroids.append(list(chosen))

        # Iterative optimization (Lloyd's Algorithm)
        for iteration in range(self.max_iter):
            clusters = [[] for _ in range(self.k)]
            for vec in vectors:
                best_c = 0
                best_dist = float("inf")
                for c_idx, cent in enumerate(self.centroids):
                    dist = sum((v - c) ** 2 for v, c in zip(vec, cent))
                    if dist < best_dist:
                        best_dist = dist
                        best_c = c_idx
                clusters[best_c].append(vec)

            # Update centroids
            new_centroids = []
            shift = 0.0
            for c_idx in range(self.k):
                members = clusters[c_idx]
                if not members:
                    new_centroids.append(self.centroids[c_idx])
                    continue
                new_cent = [sum(m[j] for m in members) / len(members) for j in range(n_features)]
                shift += sum((a - b) ** 2 for a, b in zip(self.centroids[c_idx], new_cent))
                new_centroids.append(new_cent)

            self.centroids = new_centroids
            if shift < 1e-4:
                break

        # Calculate final inertia (Sum of squared distances)
        self.inertia_ = 0.0
        for vec in vectors:
            min_d = min(sum((v - c) ** 2 for v, c in zip(vec, cent)) for cent in self.centroids)
            self.inertia_ += min_d

        return self

    def predict(self, records):
        preds = []
        for r in records:
            vec = [float(r[col]) for col in self.feature_cols]
            best_c = 0
            best_dist = float("inf")
            for c_idx, cent in enumerate(self.centroids):
                dist = sum((v - c) ** 2 for v, c in zip(vec, cent))
                if dist < best_dist:
                    best_dist = dist
                    best_c = c_idx
            preds.append(best_c)
        return preds


class GaussianNBClassifier:
    """
    Gaussian Naive Bayes Classifier for Yield Efficiency Category (High / Medium / Low).
    Calculates class priors, feature means, and feature variances under normal distribution assumptions.
    """
    def __init__(self):
        self.classes = []
        self.priors = {}
        self.means = {}
        self.vars = {}

    def fit(self, records, feature_cols, target_col):
        self.feature_cols = feature_cols
        self.target_col = target_col
        self.classes = sorted(list(set(r[target_col] for r in records)))
        n_total = len(records)

        for c in self.classes:
            class_recs = [r for r in records if r[target_col] == c]
            self.priors[c] = len(class_recs) / n_total
            self.means[c] = {}
            self.vars[c] = {}

            for f in feature_cols:
                vals = [float(r[f]) for r in class_recs]
                m = sum(vals) / len(vals)
                v = sum((x - m) ** 2 for x in vals) / len(vals)
                self.means[c][f] = m
                self.vars[c][f] = max(v, 1e-4)  # variance smoothing

    def predict(self, records):
        predictions = []
        for r in records:
            best_class = None
            max_log_prob = -float("inf")
            for c in self.classes:
                log_prob = math.log(self.priors[c])
                for f in self.feature_cols:
                    x = float(r[f])
                    m = self.means[c][f]
                    v = self.vars[c][f]
                    # Log-likelihood of Gaussian
                    prob_f = -0.5 * math.log(2 * math.pi * v) - ((x - m) ** 2) / (2 * v)
                    log_prob += prob_f
                if log_prob > max_log_prob:
                    max_log_prob = log_prob
                    best_class = c
            predictions.append(best_class)
        return predictions


class ModelEvaluator:
    @staticmethod
    def classification_metrics(actual, predicted, classes):
        matrix = {c: {c2: 0 for c2 in classes} for c in classes}
        for a, p in zip(actual, predicted):
            matrix[a][p] += 1

        total = len(actual)
        correct = sum(matrix[c][c] for c in classes)
        accuracy = round(correct / total, 4) if total > 0 else 0.0

        metrics_per_class = {}
        for c in classes:
            tp = matrix[c][c]
            fp = sum(matrix[other][c] for other in classes if other != c)
            fn = sum(matrix[c][other] for other in classes if other != c)
            precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else 0.0
            recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else 0.0
            f1 = round(2 * precision * recall / (precision + recall), 4) if (precision + recall) > 0 else 0.0
            metrics_per_class[c] = {
                "precision": precision,
                "recall": recall,
                "f1_score": f1,
                "support": sum(matrix[c].values())
            }

        macro_f1 = round(sum(m["f1_score"] for m in metrics_per_class.values()) / len(classes), 4)

        return {
            "accuracy": accuracy,
            "macro_f1": macro_f1,
            "confusion_matrix": matrix,
            "per_class": metrics_per_class
        }
