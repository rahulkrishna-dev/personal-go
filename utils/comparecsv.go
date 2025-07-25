package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"sort"
	"strings"
)

// Read CSV file and return a map of rows with their frequency
func readCSV(filePath string) (map[string][]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	dataMap := make(map[string][]string)

	// Store each row as a key (joining values) and store its content separately
	for _, record := range records {
		key := record[0] // Assuming first column as unique key (modify if needed)
		dataMap[key] = record
	}
	return dataMap, nil
}

// Compare CSV files and print detailed differences
func compareCSV(file1, file2 string) {
	data1, err := readCSV(file1)
	if err != nil {
		fmt.Println("Error reading file 1:", err)
		os.Exit(1)
	}

	data2, err := readCSV(file2)
	if err != nil {
		fmt.Println("Error reading file 2:", err)
		os.Exit(1)
	}

	var onlyInFile1, onlyInFile2, differentContent []string

	// Compare rows
	for key, row1 := range data1 {
		row2, exists := data2[key]
		if !exists {
			onlyInFile1 = append(onlyInFile1, strings.Join(row1, ","))
		} else if strings.Join(row1, ",") != strings.Join(row2, ",") {
			differentContent = append(differentContent, fmt.Sprintf("File1: %s | File2: %s", strings.Join(row1, ","), strings.Join(row2, ",")))
		}
	}

	// Find rows only in file 2
	for key, row2 := range data2 {
		if _, exists := data1[key]; !exists {
			onlyInFile2 = append(onlyInFile2, strings.Join(row2, ","))
		}
	}

	// Sort for better readability
	sort.Strings(onlyInFile1)
	sort.Strings(onlyInFile2)
	sort.Strings(differentContent)

	// Print differences
	fmt.Println("=== Rows only in File 1 ===")
	fmt.Printf("Num rows only in file 1: %v\n", len(onlyInFile1))
	for _, row := range onlyInFile1 {
		fmt.Println(row)
	}
	fmt.Println("\n=== Rows only in File 2 ===")
	fmt.Printf("Num rows only in file 2: %v\n", len(onlyInFile2))
	for _, row := range onlyInFile2 {
		fmt.Println(row)
	}

	fmt.Println("\n=== Common Rows with Different Content ===")
	for _, row := range differentContent {
		fmt.Println(row)
	}
}
