#!/usr/bin/env bash

JAR=target/index.jar
DIR=/var/corpora/covid
ES="http://localhost:9200"
SOLR="http://129.114.16.34:8983/solr"

THREADS=4
INDEX=cord_askme

function solr() {
	java -jar $JAR --input $DIR -b 1000 -t $THREADS --cord19 --core $INDEX --solr $SOLR
}

function elastic() {
	java -jar $JAR --input $DIR -b 1000 -t $THREADS --cord19 --core $INDEX --elastic $ES
}


if [[ -z $1 ]] ; then
	run
	exit
fi

if [[ -n $2 ]] ; then
	INDEX=$2
fi

case $1 in
	create)
		curl -i -X PUT -d @src/main/resources/mappings.json -H "Content-type: application/json" $ES/$INDEX
		echo ""
		;;
	delete)
		curl -i -X DELETE $ES/$INDEX
		;;	
	mappings)
		curl $ES/$INDEX/_mappings | jsonpp
		;;
	run)
		solr
		;;
	*)
		echo "Invalid option: $1"
		echo "USAGE ./index [create|delete|mappings|run]"
		echo ""
esac


