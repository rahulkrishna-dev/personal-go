package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func generatePincodeJsonChunks() {
	var pincodes [][]Pincode
	var chunk []Pincode
	for pincode, data := range PinCodeDetailsMap {
		if len(chunk) == 1000 {
			pincodes = append(pincodes, chunk)
			chunk = nil
		}
		chunk = append(chunk, Pincode{
			Pincode:      pincode,
			RetailCityId: data.RetailCityId,
			DistrictName: data.District,
			StateCode:    data.StateCode,
		})
	}
	jsonData, err := json.MarshalIndent(pincodes, "", "  ")
	if err != nil {
		fmt.Println("Error marshalling JSON:", err)
		return
	}
	writeErr := os.WriteFile("/Users/rahul.krishna@grofers.com/Desktop/pincodeChunks.json", jsonData, 0644)
	if writeErr != nil {
		fmt.Println("Error writing JSON to file:", writeErr)
		return
	}
	fmt.Println("successfully written saved pincodes.json")
}
