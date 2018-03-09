# How it works
Both classifiers use numerical input features and predict a numerical label.
There are working and self explaining examples in the test folders.

## RandomForest

### Java API 
The 'RandomForest' class contains methods to train an ensemble of decision trees according to [Breimann](https://www.stat.berkeley.edu/~breiman/randomforest2001.pdf).

There is a 'RandomForestBuilder' class for a more convenient setting of the parameters.

#### fit 
The fit takes a Java 'DataSet<Tuple2<Double, Vector<Double>>>' and trains the classifier. 

#### predict 
After the classifier is successfully trained the predict method is able to predict labels by passing it a 'DataSet<Vector<Double>>'.

#### evaluate
After the classifier is successfully trained you are able to evaluate its performance by using the evaluate function. 
It requires you to pass a 'DataSet<Tuple2<Double, Vector<Double>>>'. The features vectors will be used to predict the labels and compare them to the given labels. This returns a 'DataSet<Tuple2<Double, Double>>' where the first Double is the predicted label and the second double is the real label.
This way you can further process the data to match your desired evaluation.

#### evaluateBinaryClassification
The method uses the same parameters but assumes a binary classification with true being encoded as '1.0' and false as '-1.0'.
This way it is able to calculate accuray, precision and recall for binary classifications.

### Scala API
There is no dedicated Scala API yet. But you can find the above methods in the 'RandomForestModel' class in the 'randomforest' package.
The methods behave the same way but use Scala native datastructures.

## DecisionTree
The decision tree is implemented using information gain to find the best split.
It supports the same methods and behaves the same way but uses a distributed decision tree as the underlying classifier.

### Java API

There is a 'DecisionTreeBuilder' class for a more convenient setting of the parameters.

### Scala API
There is no dedicated Scala API yet. But you can find the above methods in the 'DecisionTreeModel' class.
