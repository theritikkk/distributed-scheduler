#!/bin/bash

# !/bin/bash is called a: Shebang 
# It tells Linux/macOS: “Run this file using Bash.” Without it, the OS won't know how to execute the script directly.

TOKEN=$1

if [ -z "$TOKEN" ]; then
  echo "Usage: ./monitor-test.sh <JWT_TOKEN>"
  exit 1
fi

echo "Flooding scheduler with demo tasks..."

FUTURE_TIME=$(date -u -v+1M +"%Y-%m-%dT%H:%M:%SZ")


for i in {1..200}
do
  curl -s -X POST http://localhost:3000/api/v1/tasks \
    -H "Authorization: Bearer $TOKEN" \
    -H "Content-Type: application/json" \
    -d "{
      \"task_name\": \"load-test-$i\",
      \"command_payload\": {
        \"command\": \"echo hello-$i\"
      },
      \"schedule_type\": \"one-time\",
      \"next_execution_time\": \"$FUTURE_TIME\"
    }" | jq .

  echo "Queued task $i"
done

echo "Done."


# chmod +x scripts/monitor-test.sh : will make this program executable, Before that: it was just text, not a runnable program

