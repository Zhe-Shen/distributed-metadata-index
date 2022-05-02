package main

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

// main function
func main() {
	debugMode := true
	tagNameList := []string{"cpu", "region", "operationName", "resultType", "level", "resourceId"}
	// the number of data to be generated
	sizeOfData := 100000

	rand.Seed(88)

	cpu := []string{"Intel", "AMD"}
	region := []string{"EastUS1", "EastUS2", "WestUS1", "WestUS2", "NorthCentralUS", "CentralUS", "SouthCentralUS", "WestCentralUS", "BrazilSouth", "CanadaCentral", "ChinaEast2", "EastAsia", "SoutheastAsia", "CentralIndia", "AustraliaEast", "AustraliaSoutheast", "NorthEurope", "WestEurope", "FranceCentral"}
	operation := []string{"Create", "Read", "Update", "Delete"}
	resultType := []string{"A", "B", "C", "D", "E", "F", "G"}
	// level: 1~9
	level := []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"}

	// link the tag key to the tag value list and stored into a map
	tagMap := make(map[string][]string)
	tagMap["cpu"] = cpu
	tagMap["region"] = region
	tagMap["operationName"] = operation
	tagMap["resultType"] = resultType
	tagMap["level"] = level
	fileName := "output_" + strconv.Itoa(sizeOfData) + ".txt"
	file, err := os.Create(fileName)
	check(err)

	for i := 0; i < sizeOfData; i++ {
		var sb strings.Builder
		for j := 0; j < len(tagNameList)-1; j++ {
			key := tagNameList[j]
			sizeOfValList := len(tagMap[key])
			// randomly choose the index of one item
			idx := rand.Intn(sizeOfValList)
			tagVal := tagMap[key][idx]
			sb.WriteString(key + "=" + tagVal)
			sb.WriteString(",")
		}
		// resourceId: 1~5 digit number
		resourceId := rand.Intn(10000)
		resourceIdVal := "resourceId=" + strconv.Itoa(resourceId) + ","
		sb.WriteString(resourceIdVal)

		// append the prefix string tag pair
		tagKey := getPrefixStr(8)
		tagVal := getPrefixStr(3)
		sb.WriteString(tagKey + "=" + tagVal)

		sb.WriteString("\n")

		if debugMode {
			fmt.Println(sb.String())
		}

		// Write the generated tag info to the txt file
		_, err := file.WriteString(sb.String())
		check(err)
	}
	defer file.Close()
}

func getPrefixStr(lengthOfString int) string {
	alphabet := "abcdefghijklmnopqrstuvwxyz"
	var sb strings.Builder
	initalProb := 9
	for i := 0; i < lengthOfString; i++ {
		subString := ""
		num := rand.Intn(10)
		if num < initalProb {
			subString = alphabet[i : i+4]
		} else {
			subString = alphabet[i+4:]
		}
		randIdx := rand.Intn(len(subString))
		sb.WriteString(string(subString[randIdx]))
		initalProb -= 1
	}
	return sb.String()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
