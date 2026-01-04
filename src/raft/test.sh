#!/bin/bash

   VERBOSE=1 go test -v *.go  -test.run=TestBackup2B  | python dslogs.py -c 5 > log

exit
times=${1:-1}
for ((i=1; i<=$times; i++))
do
    echo "Running test iteration $i..."

     go test -v -run=".*2A.*" 2>&1 | python dslogs.py -c 3 | tee test_output.log
     if [ ${PIPESTATUS[0]} -ne 0 ]; then
         echo "Test with 2A failed, stopping..."
         exit 1
     fi

     go test -v -run=".*2B.*" 2>&1 | python dslogs.py -c 3 | tee test_output.log
     if [ ${PIPESTATUS[0]} -ne 0 ]; then
         echo "Test with 2B failed, stopping..."
         exit 1
     fi

done

echo "All tests completed successfully!"
