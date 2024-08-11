def parse_input_file(filename):
    """Parses the input file and returns a dictionary of entries."""
    entries = {}
    with open(filename, 'r') as file:
        for line in file:
            key, value = line.strip().split(': ')
            entries[key] = int(value)
    return entries

def group_entries(entries):
    """Groups entries into sets such that the sum of each set is close to 1000."""
    sorted_entries = sorted(entries.items(), key=lambda x: x[1], reverse=True)
    groups = []
    current_group = []
    current_sum = 0
    
    for entry, value in sorted_entries:
        if value > 1000:
            groups.append([(entry, value)])
        elif current_sum + value <= 1000:
            current_group.append((entry, value))
            current_sum += value
        else:
            if current_group:
                groups.append(current_group)
            current_group = [(entry, value)]
            current_sum = value
    
    if current_group:
        groups.append(current_group)
    
    return groups

def main():
    filename = 'input.txt'
    entries = parse_input_file(filename)
    groups = group_entries(entries)
    
    for group in groups:
        keys = [item[0] for item in group]
        values = [item[1] for item in group]
        group_sum = sum(values)
        print(f"{', '.join(keys)}: {' + '.join(map(str, values))} = {group_sum}")

if __name__ == "__main__":
    main()
