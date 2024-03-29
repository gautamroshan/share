https://hapi.fhir.org/baseR4/PractitionerRole?_count=1


#!/bin/bash

# Function to extract _getpages parameter from a URL
extract_getpages_param() {
    url="$1"
    getpages_param=$(echo "$url" | grep -oP '_getpages=\K[^&]+')
    echo "$getpages_param"
}

# Function to extract _getpagesoffset parameter from a URL
extract_getpages_offset() {
    url="$1"
    getpages_offset=$(echo "$url" | grep -oP '_getpagesoffset=\K[^&]+')
    echo "$getpages_offset"
}

# Function to fetch and write resources from a given URL
fetch_and_write() {
    url="$1"
    output_file="$2"
    temp_file="$3"
    
    resources=$(curl -s "$url")
    
    echo "$resources" | jq -c '.entry[].resource' >> "$temp_file"
    
    next_page_url=$(echo "$resources" | jq -r '.link[] | select(.relation == "next") | .url')
    
    if [ -z "$next_page_url" ]; then
        echo "Last page reached."
        touch .last_page_flag
    else
        echo "Next page URL: $next_page_url"
    fi
}

# Function to merge temporary files into the final output file
merge_temporary_files() {
    output_file="$1"
    temp_files=("$@")

    cat "${temp_files[@]}" > "$output_file"
}

# Function to fetch resources in parallel and write to temporary files
fetch_resources_parallel() {
    base_url="$1"
    output_file="$2"
    next_url_format="$3"
    count="$4"
    total_workers="$5"
    
    temp_files=()
    current_offset=0
    while [ ! -f .last_page_flag ]; do
        # Assign worker threads to different partitions of the searchset
        for (( i = 0; i < total_workers; i++ )); do
            temp_file=$(mktemp)
            fetch_and_write "${next_url_format//_getpagesoffset=$current_offset/_getpagesoffset=$current_offset}" "$output_file" "$temp_file" &
            temp_files+=("$temp_file")
            current_offset=$(( current_offset + count ))
        done

        # Wait for all workers to complete
        wait

        # Merge temporary files
        merge_temporary_files "$output_file" "${temp_files[@]}"
        temp_files=()
    done
}

# Function to manage dynamic offset assignment
manage_offset_assignment() {
    base_url="$1"
    output_file="$2"
    total_workers="$3"
    
    # Fetch initial page to extract _getpages parameter and _count parameter
    initial_page=$(curl -s "$base_url")
    getpages_param=$(extract_getpages_param "$initial_page")
    count=$(echo "$initial_page" | jq -r '.link[] | select(.relation == "next") | .url' | grep -oP '_count=\K[^&]+')
    next_url_format=$(echo "$initial_page" | jq -r '.link[] | select(.relation == "next") | .url')
    getpages_offset_param=$(extract_getpages_offset "$next_url_format")
    
    # Pass the second page, since we're starting to parallelize from the second page, the first page is done processing.
    fetch_and_write "$base_url" "$output_file" "$base_url.tmp"
    
    # Start fetching resources in parallel
    fetch_resources_parallel "$base_url" "$output_file" "$next_url_format" "$count" "$total_workers"
}

# Main function
main() {
    base_url="https://example.com/Patient?_lastUpdated=gt2020-01-01"
    output_file="output.ndjson"
    total_workers=4
    
    # Run offset assignment
    manage_offset_assignment "$base_url" "$output_file" "$total_workers"
}

# Run the main function
main
