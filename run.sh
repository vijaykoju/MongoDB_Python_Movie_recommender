#!/bin/bash

./process_data_v2.sh ratings_line_index.txt 60001 70000 5000 True &
./process_data_v2.sh ratings_line_index.txt 70001 80000 5000 True &
./process_data_v2.sh ratings_line_index.txt 80001 90000 5000 True &
./process_data_v2.sh ratings_line_index.txt 90001 100000 5000 True &
./process_data_v2.sh ratings_line_index.txt 100001 110000 5000 True &

# ./process_data_v2.sh ratings_line_index.txt 5001 6000 100 True &
# ./process_data_v2.sh ratings_line_index.txt 6001 7000 100 True &
# ./process_data_v2.sh ratings_line_index.txt 7001 8000 100 True &
# ./process_data_v2.sh ratings_line_index.txt 8001 9000 100 True &
# ./process_data_v2.sh ratings_line_index.txt 9001 10000 100 True &
