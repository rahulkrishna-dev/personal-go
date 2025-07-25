package utils

import "fmt"

func checkMapKeyOrder() {
	// Define a map
	mp := map[string]int{}
	mp["apple"] = 1
	mp["banana"] = 2
	mp["cherry"] = 3
	mp["date"] = 4
	mp["elderberry"] = 5
	// Iterate multiple times to observe the order
	for i := 1; i <= 3; i++ {
		fmt.Printf("Iteration %d: ", i)
		for key := range mp {
			fmt.Print(key, " ")
		}
		fmt.Println()
	}
}
