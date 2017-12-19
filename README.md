This is Item based recommendation system using cosine similarity.


To run each task:

Task1:
add signature licence(if need):
zip -d /YOUR_PATH_TO_JAR_FILE/weinung_chao_task1.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

to run this program:
/YOUR_PATH_TO_SPARK /spark-1.6.1-bin-hadoop2.4/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 --class Weinung_Chao_task1 weinung_chao_task1.jar ratings.csv testing_small.csv 

Task2:
add signature licence(if need):
zip -d /YOUR_PATH_TO_JAR_FILE/weinung_chao_task1.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF

to run this program:
/YOUR_PATH_TO_SPARK /spark-1.6.1-bin-hadoop2.4/bin/spark-submit --packages com.databricks:spark-csv_2.10:1.4.0 --class Weinung_Chao_task2 weinung_chao_task2.jar ratings.csv testing_small.csv 
