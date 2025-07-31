#!/bin/bash

# Download the zip file
curl -O https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip

# Extract the contents of the zip file
unzip hdma-wi-2021.zip

# Store the CSV filename in a variable for convenience
CSV_FILE="hdma-wi-2021.csv"

# Count the lines containing the text "Multifamily" (case sensitive)
echo "Number of lines containing 'Multifamily':"
grep "Multifamily" "$CSV_FILE" | wc -l

# Count the total number of lines in the CSV file
echo "Total number of lines in the CSV file:"
wc -l < "$CSV_FILE"

# Count the lines containing the text "Single Family" (case sensitive)
echo "Number of lines containing 'Single Family':"
grep "Single Family" "$CSV_FILE" | wc -l
