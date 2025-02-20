package main

type Pincode struct {
	Pincode      string `json:"pincode"`
	RetailCityId int    `json:"retail_city_id"`
	DistrictName string `json:"district_name"`
	StateCode    int    `json:"state_code"`
}

func main() {
	generatePincodeJsonChunks()
}
