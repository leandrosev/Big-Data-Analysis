# Big Data Analysis #


## 1. Analyzing fishing activity from AIS (Automatic identification system) data ##
  
  ### Technologies: ###
    Python
    MongoDB
    
  ### Dataset: ###
    https://zenodo.org/record/1167595#.X_0AltgzaUl
    Refers to real data from October 1st,2015 to March 31,2016, in Celtic Sea, Channel and Bay of Biscay (France).
    
   ![data](https://ars.els-cdn.com/content/image/1-s2.0-S2352340919304950-gr1.jpg)
    
    
  ### Topics: ###
    Fishing activity report
    Fishing vessels trajectory clustering (using Hausdorff distance)
    Fishing spots clustering
    Finding illegal fishing activities
    Clustering algorithms used: KMeans,DBSCAN,Optics,Birch
    
  ### Files: ###
    FishingAnalysisAIS.pdf (Greek)
    nosql_db.ipynb (Add data to local mongodb )
    mongodbProject.ipynb 

## 2. Geospatial Queries on Big Data  ##
  
  ### Technologies ###
    Apache Spark
    Scala
 ![sparkscala](https://miro.medium.com/max/698/1*joLOATG-6WgXD-2Q22tzkQ.jpeg)
 
  ### Dataset: ###
    Testing dataset: Hotels and Restaurants Worldwide
    Any two geospatial datasets can be used with a few modifications of the code
    Data can be downloaded from https://www.dropbox.com/s/dis84tm0r5vyzy7/hotels_restaurants.rar?dl=0
  
  ### Topics ###
    Geospatial data indexing and partitioning in a cluster
    Geospatial join between two datasets given a distance d
      i.e.: Find all hotels close to all restaurants by a distance no more than d
    Manual creation of a space-partitioning algorithm (for learning purposes. Well known libraries already exist for this task)
    For distance metric, Haversine formula is used
    
  ### Files ###
    GeoQueries_Spark.jar 
            or
    GeoQueries_Spark.scala
  
  ### Instructions (for jar) ###
   run: spark-submit --master local[*] --class "app"  JAR_FILE d n file1 file2
   where:
      JAR_FILE:  jar file location
      d: distance in kilometers
      n: number of partitions to make
      file1,file2: locations of the two datasets to join
  

  
