from sklearn.datasets import load_iris


iris = load_iris(as_frame=True)
dataset = iris.data.assign(
    target=iris.target
)
dataset.to_parquet("iris.parquet")
dataset.to_csv("iris.csv", index=False)
