#!/bin/bash

# Log4j 2.x is used in DBR 11.0 and later.
# Log4j 2.x uses an XML file for configuration.
LOG4J2_PATH="/home/ubuntu/databricks/spark/dbconf/log4j/driver/log4j2.xml"

# Check if we are on the driver node and found the log4j2.xml file.
if [ -f "$LOG4J2_PATH" ]; then
   # Echo the original file so we can see what we're modifying.
   cat $LOG4J2_PATH

   sed -i 's|<PatternLayout[^>]*/>|<JsonTemplateLayout eventTemplateUri="classpath:LogstashJsonEventLayoutV1.json"/>|g' $LOG4J2_PATH
fi
