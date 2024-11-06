#!/bin/sh

# Configuration
ES_URL="http://localhost:9200"
INDEX="sample_data"
SCROLL_TIME="5m"
MAX_RETRIES=3
RETRY_DELAY=2
BATCH_SIZE=10
OUTPUT_FILE="titles_output.json"
JQ_CMD=$(command -v jq)

# Check if jq is available
if [ -z "$JQ_CMD" ]; then
    echo "Error: jq is not installed. Please install jq for better JSON handling." >&2
    exit 1
fi

# Initialize output file with an empty JSON array
# echo "[" > "$OUTPUT_FILE"

# Function to make an HTTP request, capture both the response body and status code,
# and handle errors based on the status code.
make_request() {
    local response
    # Run curl silently, appending the HTTP status code at the end of the output.
    response=$(curl -s -w "\n%{http_code}" "$@")
    
    # Extract the HTTP status code (last line of response).
    local status_code=$(echo "$response" | tail -n1)
    
    # Extract the response body (all lines except the last).
    local body=$(echo "$response" | sed '$d')
    
    # If status code is 200 (OK), print the body; otherwise, output an error message and return 1.
    if [ "$status_code" -eq 200 ]; then
        echo "$body"
    else
        echo "Error: HTTP $status_code" >&2
        echo "$body" >&2
        return 1
    fi
}


# Function to fetch a batch of data from Elasticsearch using a scroll ID,
# retrying with exponential backoff if the request fails.
fetch_scroll_batch() {
    local scroll_id="$1"         # Scroll ID passed as the first argument
    local attempt=1              # Initialize attempt counter
    local delay="$RETRY_DELAY"   # Set initial retry delay from configuration
    local response               # Variable to store the response

    # Loop to attempt fetching the scroll batch, up to the maximum retry limit
    while [ "$attempt" -le "$MAX_RETRIES" ]; do
        echo "Fetching batch with scroll ID (Attempt $attempt, Delay $delay seconds)" >&2
        
        # Make the request using the scroll ID, specifying scroll duration and headers
        response=$(make_request -X GET "$ES_URL/_search/scroll" \
            -H "Content-Type: application/json" \
            -d '{
                "scroll": "'"$SCROLL_TIME"'",
                "scroll_id": "'"$scroll_id"'"
            }')
        
        # Check if the request succeeded and if there are hits in the response
        if [ $? -eq 0 ] && echo "$response" | jq -e '.hits.hits' >/dev/null; then
            echo "$response"    # Return the response on success
            return 0
        else
            # Log failure and apply exponential backoff delay before retrying
            echo "Attempt $attempt failed. Retrying in $delay seconds..." >&2
            sleep "$delay"
            delay=$((delay * 2))  # Double the delay for exponential backoff
            attempt=$((attempt + 1))
        fi
    done

    # Log failure after all retry attempts are exhausted
    echo "Failed to fetch after $MAX_RETRIES attempts." >&2
    return 1
}


# Function to clean up the scroll context in Elasticsearch by deleting the scroll ID,
# which frees up resources on the Elasticsearch server.
cleanup_scroll() {
    local scroll_id="$1"  # Scroll ID to be cleaned up, passed as the first argument
    echo "Cleaning up scroll context..." >&2  # Log the cleanup action
    
    # Send a DELETE request to Elasticsearch to remove the scroll context
    make_request -X DELETE "$ES_URL/_search/scroll" \
        -H "Content-Type: application/json" \
        -d '{
            "scroll_id": ["'"$scroll_id"'"]
        }' >/dev/null  # Suppress output to prevent clutter
}


# Set a trap to automatically run `cleanup_scroll` with the scroll ID when the script exits,
# ensuring that the scroll context is cleaned up regardless of how the script terminates.
# trap: The trap command in shell scripting is used to specify a command (or series of commands) to be executed automatically when the script exits or encounters certain signals.
# EXIT: This is a built-in signal that triggers when the script exits, whether due to completion, an error, or manual interruption.
# cleanup_scroll "$scroll_id": This is the command that will be executed on exit. Here, it calls cleanup_scroll with the current scroll_id, ensuring that the scroll context in Elasticsearch is deleted, freeing up resources.
trap 'cleanup_scroll "$scroll_id"' EXIT


# Log the start of the scroll search initialization to standard error
echo "Initiating scroll search..." >&2

# Make an initial scroll request to Elasticsearch to retrieve the first batch of results
# Set the batch size to control the number of documents per batch
# Limit the response to include only the 'title' field in each document
# Use sorting by '_doc' for efficient sequential access to the documents
initial_response=$(make_request -X GET "$ES_URL/$INDEX/_search?scroll=$SCROLL_TIME" \
    -H "Content-Type: application/json" \
    -d '{
        "size": '"$BATCH_SIZE"',
        "_source": ["title"],
        "sort": ["_doc"]
    }')

# Check if the initial request was successful by verifying the exit status ($?)
if [ $? -ne 0 ]; then
    echo "Failed to initiate scroll search" >&2
    exit 1
fi

# Extract scroll ID using jq
scroll_id=$(echo "$initial_response" | jq -r '._scroll_id')

if [ -z "$scroll_id" ] || [ "$scroll_id" = "null" ]; then
    echo "Error: Failed to retrieve a valid scroll ID." >&2
    exit 1
fi

# Function to process and output titles from the JSON response.
# Extracts each title, displays it on the console, and appends it to an external file in JSON format.
process_titles() {
    local response="$1"  # The JSON response containing document data is passed as the first argument
    # Use jq to extract all 'title' fields from the response, one per line
    local titles=$(echo "$response" | jq -r '.hits.hits[]._source.title')
    
    # Check if titles were found in the response
    if [ -n "$titles" ]; then
        # Loop through each extracted title
        echo "$titles" | while IFS= read -r title; do
            echo "$title"        # Display title on console
            echo "\"$title\"" >> "$OUTPUT_FILE"  # Append title to output file in JSON format
        done
        return 0  # Indicate success
    fi
    return 1  # Indicate failure if no titles were found
}


# Process initial batch
process_titles "$initial_response"

# Fetch remaining batches in a loop until there are no more documents
while true; do
    # Fetch the next batch of results using the current scroll ID
    response=$(fetch_scroll_batch "$scroll_id")
    
    # Check if fetch was successful by evaluating the exit status
    if [ $? -ne 0 ]; then
        echo "Error fetching next batch" >&2  # Log an error if the fetch failed
        exit 1                                # Exit with an error code
    fi
    
    # Extract and update the new scroll ID from the response for the next batch
    new_scroll_id=$(echo "$response" | jq -r '._scroll_id')
    # If the new scroll ID is empty or invalid, log an error and exit
    if [ -z "$new_scroll_id" ] || [ "$new_scroll_id" = "null" ]; then
        echo "Error: Invalid scroll ID received" >&2
        exit 1
    fi
    scroll_id="$new_scroll_id"  # Update scroll_id for the next iteration
    
    # Process titles in the current batch and check for end of data
    if ! process_titles "$response"; then
        echo "All documents retrieved." >&2  # Log message indicating end of results
        break  # Exit the loop if no more titles are found
    fi
done


# Finalize JSON format in output file
sed -i '' -e '$ s/,$//' "$OUTPUT_FILE"  # Remove trailing comma from the last entry
# echo "]" >> "$OUTPUT_FILE"              # Close the JSON array

echo "All titles have been saved to $OUTPUT_FILE in JSON format."
exit 0
