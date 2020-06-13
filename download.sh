#!/usr/bin/env bash

DATE="2020-04-03"

ROOT="https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com"
COM="comm_use_subset.tar.gz"
NONCOM="noncomm_use_subset.tar.gz"
CUST="custom_license.tar.gz"
XIV="biorxiv_medrxiv.tar.gz"

case $1 in
	-d|-date|--date)
		DATE=$1
		shift
		;;
esac

file=$NONCOM
while [ -n "$1" ] ; do
	case $1 in
		comm|com) file=$COM ;;
		non|noncom) file=$NONCOM ;;
		cust) file=$CUST ;;
		xiv) file=$XIV ;;
		*)
			echo "Invalid selection $1"
			echo "Must be one of com, non, cust, or xiv"
			exit 1
			;;
	esac
	wget $ROOT/$DATE/$file
	tar -xzvf $file
	shift
done

		
