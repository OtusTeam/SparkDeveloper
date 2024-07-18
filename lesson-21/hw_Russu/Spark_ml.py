"""
Обучение модели классификации на примере датасета инструментами
Ссылка на датасет: https://www.kaggle.com/datasets/arshid/iris-flower-dataset
"""
import pickle

from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql.types import StructType, FloatType, StructField, StringType
import os
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel


def build_evaluator() -> MulticlassClassificationEvaluator:
    return MulticlassClassificationEvaluator(labelCol="indexed_species", predictionCol="prediction",
                                             metricName="accuracy")


def build_tvs(rand_forest, evaluator, pipeline) -> TrainValidationSplit:
    paramGrid = ParamGridBuilder() \
        .addGrid(rand_forest.maxDepth, [2, 3, 4, 5]) \
        .addGrid(rand_forest.maxBins, [2, 3, 4]) \
        .build()

    return TrainValidationSplit(estimator=pipeline,
                                estimatorParamMaps=paramGrid,
                                evaluator=evaluator,
                                trainRatio=0.8)


def train_model(train_df, test_df) -> (RandomForestClassificationModel, float):
    assembler = VectorAssembler(inputCols=features, outputCol="features")
    train_pdf = assembler.transform(train_df)
    test_pdf = assembler.transform(test_df)
    rf = RandomForestClassifier(labelCol="indexed_species", featuresCol="features")
    evaluator = build_evaluator()
    from pyspark.ml import Pipeline
    pipeline = Pipeline(stages=[rf])
    tvs = build_tvs(rf, evaluator, pipeline=pipeline)

    models = tvs.fit(train_pdf)
    best = models.bestModel
    predictions = best.transform(test_pdf)
    predictions.show()

    accuracy = evaluator.evaluate(predictions)
    print(f"Accuracy: {accuracy}")
    jo = models.bestModel.stages[-1]._java_obj
    print(f'Model maxDepth: {jo.getMaxDepth()}')
    print(f'Model maxBins: {jo.getMaxBins()}')
    return best, accuracy


if __name__ == "__main__":
    spark = SparkSession.builder \
        .config('spark.driver.memory', '8g') \
        .config('spark.executor.memory', '12g') \
        .config('spark.sql.shuffle.partitions', 1000) \
        .config('spark.sql.debug.maxToStringFields', 1000) \
        .config('spark.executor.cores', '4') \
        .config('spark.executor.instances', '3') \
        .appName('MLiF_scoring').getOrCreate()

    schema = StructType([StructField('sepal_length', FloatType(), True),
                         StructField('sepal_width', FloatType(), True),
                         StructField('petal_length', FloatType(), True),
                         StructField('petal_width', FloatType(), True),
                         StructField('species', StringType(), True)
                         ])
    source_df = spark.read.format("csv").option('header', True).schema(schema).load(
        os.getcwd() + '/iris.csv')
    from pyspark.ml.feature import StringIndexer

    indexer = StringIndexer(inputCol="species", outputCol="indexed_species")
    indexed_df = indexer.fit(source_df).transform(source_df)

    features = ['sepal_length', 'sepal_width', 'petal_length', 'petal_width']

    train_df, test_df = indexed_df.randomSplit([0.8, 0.2])
    model, accuracy = train_model(train_df, test_df)
    with open('model.pickle', 'wb') as file:
        pickle.dump(model, file)

