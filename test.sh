#!/bin/bash

# Function to fetch resources from a given URL and write to the output file
fetch_and_write() {
    url="$1"
    output_file="$2"
    
    # Fetch resources from the URL and write to the output file
    resources=$(curl -s "$url")
    echo "$resources" >> "$output_file"
    
    # Check if this is the last page by inspecting the link section
    if ! grep -q '"relation": "next"' <<< "$resources"; then
        echo "Last page reached."
        touch .last_page_flag
    fi
}

# Function to assign offsets to worker threads
assign_offsets() {
    base_url="$1"
    output_file="$2"
    last_offset=0
    
    # Infinite loop until last page flag is set
    while [ ! -f .last_page_flag ]; do
        # Assign offsets and URLs to worker threads
        for ((i=1; i<=4; i++)); do
            next_offset=$((last_offset + (i - 1) * 50))
            next_url="${base_url}&_getpagesoffset=${next_offset}&_count=1&_bundletype=searchset"
            echo "$next_url" >> .worker_${i}_url
        done
        
        # Wait for a short while before reassigning offsets
        sleep 1
        
        # Update last offset using the last URL assigned
        last_offset=$((last_offset + 200))
    done
}

# Main function
main() {
    base_url="https://example.com/Patient?_lastUpdated=gt2020-01-01"
    output_file="output.ndjson"
    
    # Fetch initial page and write to output file
    fetch_and_write "$base_url" "$output_file"
    
    # Assign offsets to worker threads
    assign_offsets "$base_url" "$output_file"
    
    # Start worker threads using GNU Parallel
    parallel -j 4 fetch_and_write :::: .worker_{1..4}_url "$output_file"
    
    # Cleanup worker URLs files
    rm -f .worker_*.url
    rm -f .last_page_flag
}

# Run the main function
main
