spark-submit --master yarn --deploy-mode cluster \
--py-files capstone_lib.zip \
--files conf/sdbl.conf,conf/spark.conf,log4j.properties \
main.py qa 2022-08-02



