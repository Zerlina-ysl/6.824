
times=${1:-1}
for ((i=1;i<=$times;i++))
do
#  rm -f tmp/*
#  go test -v *.go  -test.run=TestInitialElection2A
#  if grep -q "FAIL:" /tmp/test_output.log; then
#     exit 1
#  fi
  rm -f tmp/*
  go test -v *.go  -test.run=TestReElection2A  | tee /tmp/test_output.log
  if grep -q "FAIL:" /tmp/test_output.log; then
     exit 1
  fi
#    rm -f tmp/*
#  go test -v *.go  -test.run=TestManyElections2A
done

