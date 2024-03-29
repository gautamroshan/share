#!/bin/bash

# Function to calculate the next offset based on current offset and batch size
calculate_next_offset() {
    current_offset="$1"
    batch_size="$2"
    next_offset=$((current_offset + batch_size))
    echo "$next_offset"
}

# Function to fetch and write resources from a given URL
fetch_and_write() {
    url="$1"
    output_file="$2"
    
    # Fetch resources from the URL
    resources=$(curl -s "$url")
    
    # Extract each resource and write to the output file
    echo "$resources" | jq -c '.entry[].resource' >> "$output_file"
    
    # Check if there is a next page
    next_page_url=$(echo "$resources" | jq -r '.link[] | select(.relation == "next") | .url')
    
    if [ -z "$next_page_url" ]; then
        echo "Last page reached."
        touch .last_page_flag
    fi
}

# Function to fetch resources in parallel
fetch_resources_parallel() {
    base_url="$1"
    output_file="$2"
    batch_size="$3"
    
    current_offset=0
    while [ ! -f .last_page_flag ]; do
        # Calculate next offset
        next_offset=$(calculate_next_offset "$current_offset" "$batch_size")
        
        # Construct next page URL with the calculated offset
        next_page_url="${base_url}&_getpagesoffset=${next_offset}&_count=${batch_size}&_bundletype=searchset"
        
        # Fetch and write resources from the next page
        fetch_and_write "$next_page_url" "$output_file" &
        
        # Update current offset for the next iteration
        current_offset="$next_offset"
    done
    wait
}

# Main function
main() {
    base_url="https://example.com/Patient?_lastUpdated=gt2020-01-01"
    output_file="output.ndjson"
    batch_size=50
    
    # Fetch initial page and write to output file
    fetch_and_write "$base_url" "$output_file"
    
    # Spawn parallel workers to fetch resources
    for ((i=1; i<=4; i++)); do
        fetch_resources_parallel "$base_url" "$output_file" "$batch_size" &
    done
    wait
}

# Run the main function
main
