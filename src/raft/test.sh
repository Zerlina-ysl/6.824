
times=${1:-1}
for ((i=1;i<=$times;i++))
do
#  rm -f tmp/*
# VERBOSE=1 go test -v *.go  -test.run=TestInitialElection2A | python dslogs.py -c 3 > log1
# VERBOSE=1 go test -v *.go  -test.run=TestReElection2A | python dslogs.py -c 3 > log1
# VERBOSE=1 go test -v *.go  -test.run=TestManyElections2A | python dslogs.py -c 3 > log1


# VERBOSE=1 go test -v *.go  -test.run=TestBasicAgree2B | python dslogs.py -c 3 > log1
#VERBOSE=1 go test -v *.go  -test.run=TestRPCBytes2B | python dslogs.py -c 3 > log1

VERBOSE=1 go test -v *.go  -test.run=TestFailAgree2B | python dslogs.py -c 3 > log1
#  if grep -q "FAIL:" /tmp/test_output.log; then
#     exit 1
#  fi

done

