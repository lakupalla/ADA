#!/bin/sh
PATH_HDFS_INPUT=$1;
PATH_HDFS_SECURE=$2;

DATEFOLDER=$(date +%Y%m%d)
HOURFOLDER=$(date +%H)

echo "*****************************************************************************************"
echo "Move To Secure Job Started on = "$(date +%Y%m%d-%H%M%S)
echo "*****************************************************************************************"

kinit -k -t ./$KERBEROS_KEYTAB_FILE $KERBEROS_PRINCIPAL

if test "$(hadoop fs -ls "$PATH_HDFS_INPUT")"; then
	echo "Creating Folder $PATH_HDFS_SECURE/$DATEFOLDER/$HOURFOLDER/"
	hadoop fs -mkdir -p $PATH_HDFS_SECURE/$DATEFOLDER/$HOURFOLDER/
	
	echo "Moving files from $PATH_HDFS_INPUT to $PATH_HDFS_SECURE/$DATEFOLDER/$HOURFOLDER/"
	hadoop fs -mv $PATH_HDFS_INPUT/* $PATH_HDFS_SECURE/$DATEFOLDER/$HOURFOLDER/
else 
	echo "No data exists in $PATH_HDFS_INPUT skipping action."
fi
echo "*****************************************************************************************"
echo "Process Complete"
echo "*****************************************************************************************"
