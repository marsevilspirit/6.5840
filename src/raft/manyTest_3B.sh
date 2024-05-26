count=0
success_count=0
fail_count=0

#测试次数
max_tests=50

for ((i=1; i<=max_tests; i++))
do
    echo "Running test iteration $i of $max_tests..."

    go test -v -run 3B -race &> output.log

    if ["$?" -eq 0]; then
        success_count=$((success_count + 1))
        echo "Test iteration $i passed."
    else
        fail_count=$((fail_count + 1))
        mv output.log failure_$i.log
        echo "Test iteration $i failed, check failure_$i.log for more information."
    fi 
done

echo "Testing completed: $max_tests iteration run."

if ["$fail_count" -eq 0]; then
    echo "All tests passed."
else
    echo "$fail_count tests failed."
fi

rm output.log
