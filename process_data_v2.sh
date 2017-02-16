#!/bin/bash

# Input argument list:
# First argument,  $1 (Required, string) : input file
# Second argument, $2 (Required, integer): starting line index
# Third argument,  $3 (Required, integer): ending line index
# Fourth argument, $4 (Required, integer): step-size
# Fifth argument,  $5 (Required, boolean): True  --> dump data to mongodb
#                                          False --> do not dump data to mongodb

# USAGE:
# ./process_data_v2.sh ratings_line_index.txt 1 25 5 True


file=$1  # get the filename from the first argument
i=1
# read the starting and ending line indices for each user.
# the line indices correspond to the line numbers of 'ratings.csv'.
while IFS= read line
do
		array[$i]=$line
		((i++))
done <"$file"

# echo ${#array[@]}  # = 259138

# get the appropriate data chunk from 'ratings.csv', clean it and set it in json
# format and dump it into mongodb (optional). Use python to do the job.
j=0
for (( i=$2; i<=$(($2 + ($3 - $2) / $4)); i++ ))
do 
		a=${array[$(($2 + $4*$j))]}
		b=$((${array[$(($2 + $4*($j + 1)))]} - 1))
		# sed -n "${a},${b}p" ratings.csv
		python2 data_to_mongodb_v2.py -s $a -e $b -d $5
		((j++))
done
