##!/bin/bash
#for ((i=1; i<=10; i++))
#do
#    VERBOSE=1 go test -race -v *.go  -test.run=TestManyElections2A  | python dslogs.py -c 5 > log
#    if [ ${PIPESTATUS[0]} -ne 0 ]; then
#        echo "Test failed, stopping..."
#        exit 1
#    fi
#done
#exit
times=${1:-1}
for ((i=1; i<=$times; i++))
do
    echo "Running test iteration $i..."

     go test -v -run=".*2A.*" 2>&1
     if [ ${PIPESTATUS[0]} -ne 0 ]; then
         echo "Test with 2A failed, stopping..."
         exit 1
     fi

     go test -v -run=".*2B.*" 2>&1
     if [ ${PIPESTATUS[0]} -ne 0 ]; then
         echo "Test with 2B failed, stopping..."
         exit 1
     fi

done

echo "All tests completed successfully!"
