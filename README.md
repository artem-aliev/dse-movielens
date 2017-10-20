Movie Lens with Gremlin
=======================

* Marko's presentation from NoSQL Now 2015 http://www.slideshare.net/slidarko/the-gremlin-traversal-language

# Prerequisites

* Datastax DSE 5.1 or later https://docs.datastax.com/en/dse/5.1/dse-admin/datastax_enterprise/install/installTOC.html
* DSE graph loader https://docs.datastax.com/en/dse/5.1/dse-dev/datastax_enterprise/graph/dgl/dglInstall.html 

# Download MovieLens Data Set

http://grouplens.org/datasets/movielens/

```
curl -O http://files.grouplens.org/datasets/movielens/ml-1m.zip
unzip ml-1m.zip
```

# Start DSE
```
dse cassandra -k -g
```

# Load Data

```
dse gremlin-console -e schema.groovy
graphloader -graph movielens -address  127.0.0.1 movielens_loader.groovy
```

# Run examples

## TinkerPop

start dse gremlin console or studio
```
dse gremlin-console
```

Copy paste queries from tinkerPopExamples.groovy

## Spark

start dse spark
```
dse spark
```
Copy paste queries from sparkExamples.scala

